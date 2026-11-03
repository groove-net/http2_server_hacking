> **Historical document.** This file describes an earlier architecture and API.
> For the current server, use `include/http2_server.h`, `include/http_exchange.h`,
> and the `src/http_mw_*` middleware modules.

# TLS transport integration — exhaustive diff guide

This document describes every added and modified line in the server module for
the TLS transport work. The TLS module itself is new (`include/tls_server.h`,
`src/tls_server/tls_server.c`), so it has no old version to diff.

## What changed conceptually

There are now two transport paths:

1. **Plaintext, unchanged API:** `http2_session_new()` keeps using direct
   `write()` / `writev()` on the client fd. The existing response functions are
   untouched.
2. **TLS transport:** `http2_session_new_transport()` accepts a `write` and
   `writev` sink. The server passes TLS-backed callbacks. The response API is
   the same; only the destination of framed output changes.

This preserves `http2_respond_msg()` and the other response functions. The
"encrypted" and "unencrypted" response variants are selected by session
construction, not by duplicating the response API. Calling the existing response
API on a TLS session produces encrypted output; on a plain session it produces
unencrypted output.

Runtime flow:

```text
inbound:
  socket -> async read ring -> tls_conn_feed -> OpenSSL memory BIO
         -> tls_conn_read -> plaintext buffer -> http2_session_feed

outbound:
  http2 response -> plaintext transport callbacks -> tls_conn_write
                 -> OpenSSL write BIO
                 -> tls_conn_flush -> socket (direct BIO memory)
```

The TLS module uses OpenSSL memory BIOs so the async engine remains the sole
owner of socket reads. HTTP/2 never sees encrypted bytes.

## Diff counts

| File | Added | Removed/replaced |
|---|---:|---:|
| `src/server/client_session.c` | 174 | 11 |
| `src/server/server.c` | 8 | 0 |
| `src/http2/http2.c` | 49 | 18 |
| `include/http2_server.h` | 46 | 16 |
| `Makefile` | 4 | 2 |
| **New `include/tls_server.h`** | 138 | — |
| **New `src/tls_server/tls_server.c`** | 358 | — |

Only **11 old lines** are removed in the server module. They are replaced by the
four modified blocks explained below. Everything else in the server module is
purely additive.

## Server module — section by section

### Additive: TLS include and global state

`client_session.c` now includes `tls.h` and keeps one process-wide
`tls_config_t *g_tls_config`. The config is created once during server startup
and shared read-only by all workers. `server_tls_init()` is a no-op when neither
`H2_TLS_CERT` nor `H2_TLS_KEY` is set, enabling plaintext mode; if exactly one is
set it fails startup, avoiding an accidental plaintext fallback.

### Additive: per-connection TLS state

`client_ctx_t` gains:

- `tls_conn_t *tls` — per-connection OpenSSL state.
- `uint8_t *plain_buffer` / `size_t plain_size` — decrypted staging buffer.
- `int tls_ready` — handshake gate; HTTP/2 is not created before this is 1.

These fields are zeroed by the existing `calloc`, so plaintext connections keep
`tls == NULL` and follow the old path.

### Additive: TLS helper functions

- `client_tls_write()` / `client_tls_writev()` adapt the TLS module functions to
  the `http2_transport_t` callback signatures.
- `client_tls_feed_encrypted()` moves encrypted bytes from the async engine ring
  into OpenSSL, handling the ring wrap as two spans.
- `client_tls_read_plain()` repeatedly decrypts into `plain_buffer` and feeds the
  plaintext to `http2_session_feed()`.
- `client_tls_flush()` pushes pending encrypted bytes to the socket.
- `client_note_activity()` is the existing activity-counter update extracted into
  one helper so both plain and TLS paths behave identically.

### Additive: connection setup

TLS objects are allocated only when `g_tls_config != NULL`. The new
`plain_buffer` is the same size as the existing 16 KiB read buffer. Both
allocation-failure paths free the new objects before the existing cleanup.
The existing task setup is not otherwise changed.

### Modified block 1: choose HTTP/2 transport

Old:

```c
ctx->h2 = http2_session_new_ex(task->fd, g_initial_window, g_conn_window);
```

New:

```c
if (ctx->tls != NULL) {
  http2_transport_t transport = {
      .ctx = ctx->tls,
      .write = client_tls_write,
      .writev = client_tls_writev,
  };
  ctx->h2 = http2_session_new_transport(&transport, task->fd,
                                        g_stream_window, g_connection_window);
} else {
  ctx->h2 = http2_session_new(task->fd, g_stream_window, g_connection_window);
}
```

This is the only place where HTTP/2 chooses its output sink. The plaintext call
is still present and unchanged in behavior. The transport struct is copied by
the HTTP/2 constructor, so the stack-local struct is safe.

### Modified block 2: TLS handshake gate and plaintext read path

A TLS connection is fed encrypted bytes and driven through
`tls_conn_handshake()` before HTTP/2 is created. Until the handshake completes,
the handler returns after flushing handshake output or waiting for more input.
This prevents the HTTP/2 SETTINGS preface from being written before TLS is
ready.

After the handshake, encrypted bytes from the async ring go through
`client_tls_feed_encrypted()`, then `client_tls_read_plain()` decrypts and feeds
HTTP/2. The old direct-feed block is now:

```c
} else if (task->bytes_buffered > 0) {
```

so it runs only for plaintext sessions. The old inline activity update is
replaced by the identical `client_note_activity()` call.

### Modified block 3: flush and write-interest accounting

After `http2_flush()`, a TLS session calls `client_tls_flush()` to push encrypted
bytes to the socket. `want_write` now also becomes true when the TLS output
buffer is non-empty, even if nghttp2 itself has no pending frames. This is needed
because OpenSSL may hold encrypted handshake or ticket bytes after nghttp2 has
accepted the plaintext.

### Modified block 4: explicit default receive window

The old server default was `g_initial_window = 0`, meaning “let nghttp2 choose
65,535.” The constructor now rejects zero, so the default is explicit:

```c
// old
static uint32_t g_initial_window = 0;
g_initial_window = env_window("H2_INITIAL_WINDOW_SIZE", 0);

// new
static uint32_t g_stream_window = HTTP2_DEFAULT_WINDOW_SIZE;
g_stream_window = env_window("H2_INITIAL_WINDOW_SIZE",
                             HTTP2_DEFAULT_WINDOW_SIZE);
```

`Server_Init()` also rejects a zero `H2_INITIAL_WINDOW_SIZE` or
`H2_CONN_WINDOW_SIZE` before accepting connections. This keeps the constructor
contract explicit instead of silently substituting a default.

### Additive: cleanup

`tls_conn_shutdown()`, `tls_conn_flush()`, `tls_conn_free()`, and
`free(ctx->plain_buffer)` are added before `client_unlink()` in the continuation
callback. The shutdown is best-effort: it emits close_notify if the handshake
completed, flushes whatever is immediately writable, then frees the connection.
The existing HTTP/2 free and read-buffer free remain in the same order. TLS is
freed before the engine closes the fd.

## Full diff — `src/server/client_session.c`

```diff
--- a/src/server/client_session.c
+++ b/src/server/client_session.c
@@ -1,6 +1,7 @@
 #include "../../include/async_engine.h"
 #include "../../include/config.h"
 #include "../../include/http2_server.h"
+#include "../../include/tls_server.h"
 #include <limits.h>
 #include <netinet/in.h>
 #include <netinet/tcp.h>
@@ -23,13 +24,18 @@
 static uint64_t g_drain_timeout_ms = 10000;
 static uint64_t g_keepalive_ms = 0;
 static uint64_t g_keepalive_timeout_ms = 10000;
-static uint32_t g_initial_window = 0;
-static uint32_t g_conn_window = 4194304;
+static uint32_t g_stream_window = HTTP2_DEFAULT_WINDOW_SIZE;
+static uint32_t g_connection_window = 4194304;
 static size_t g_unary_budget = 33554432;         /* per connection */
 static size_t g_global_unary_budget = 268435456; /* process-wide */
+static tls_config_t *g_tls_config = NULL;
 
 typedef struct client_ctx {
   http2_session_t *h2;
+  tls_conn_t *tls;
+  uint8_t *plain_buffer;
+  size_t plain_size;
+  int tls_ready;
   io_task_t *task;
   uint64_t last_activity_ms;
   uint64_t last_app_activity_ms;
@@ -101,13 +107,37 @@
   if (g_keepalive_timeout_ms == 0) {
     g_keepalive_timeout_ms = g_keepalive_ms;
   }
-  g_initial_window = env_window("H2_INITIAL_WINDOW_SIZE", 0);
-  g_conn_window = env_window("H2_CONN_WINDOW_SIZE", 4194304);
+  g_stream_window = env_window("H2_INITIAL_WINDOW_SIZE", HTTP2_DEFAULT_WINDOW_SIZE);
+  g_connection_window = env_window("H2_CONN_WINDOW_SIZE", 4194304);
   g_unary_budget = env_size("H2_MAX_UNARY_BYTES_PER_CONN", 33554432);
   g_global_unary_budget = env_size("H2_MAX_UNARY_BYTES", 268435456);
   http2_set_global_unary_budget(g_global_unary_budget);
 }
 
+int server_tls_init(void) {
+  const char *cert = getenv("H2_TLS_CERT");
+  const char *key = getenv("H2_TLS_KEY");
+  if (cert == NULL && key == NULL) {
+    return 0;
+  }
+  if (cert == NULL || key == NULL) {
+    log_error("[tls] H2_TLS_CERT and H2_TLS_KEY must both be set");
+    return -1;
+  }
+  g_tls_config = tls_config_new(cert, key);
+  if (g_tls_config == NULL) {
+    log_error("[tls] failed to initialize TLS context");
+    return -1;
+  }
+  log_info("[tls] enabled for new connections");
+  return 1;
+}
+
+void server_tls_free(void) {
+  tls_config_free(g_tls_config);
+  g_tls_config = NULL;
+}
+
 int client_conn_admit(int fd) {
   if (g_max_conn <= 0) {
     atomic_fetch_add_explicit(&g_conn_count, 1, memory_order_relaxed);
@@ -341,6 +371,60 @@
 void my_mid_flight_handler(io_task_t *task);
 void my_continuation_callback(void *context, int status, uint8_t *buffer);
 
+static ssize_t client_tls_write(void *context, const uint8_t *buf, size_t len) {
+  return tls_conn_write(context, buf, len);
+}
+
+static ssize_t client_tls_writev(void *context, const struct iovec *iov,
+                                 int iovcnt) {
+  return tls_conn_writev(context, iov, iovcnt);
+}
+
+static int client_tls_feed_encrypted(client_ctx_t *ctx, io_task_t *task) {
+  if (task->bytes_buffered == 0) {
+    return 0;
+  }
+  ctx->last_activity_ms = now_ms();
+  size_t buffered = task->bytes_buffered;
+  size_t span = task_read_span(task);
+  const uint8_t *head = task->read_buffer + task->read_head;
+  if (tls_conn_feed(ctx->tls, head, span) != (ssize_t)span ||
+      (span < buffered &&
+       tls_conn_feed(ctx->tls, task->read_buffer, buffered - span) !=
+           (ssize_t)(buffered - span))) {
+    return -1;
+  }
+  task_read_consume(task, buffered);
+  return 0;
+}
+
+static int client_tls_read_plain(client_ctx_t *ctx) {
+  for (;;) {
+    ssize_t n = tls_conn_read(ctx->tls, ctx->plain_buffer, ctx->plain_size);
+    if (n < 0) {
+      return -1;
+    }
+    if (n == 0) {
+      return 0;
+    }
+    if (http2_session_feed(ctx->h2, ctx->plain_buffer, (size_t)n) != 0) {
+      return -1;
+    }
+  }
+}
+
+static int client_tls_flush(client_ctx_t *ctx) {
+  return tls_conn_flush(ctx->tls) < 0 ? -1 : 0;
+}
+
+static void client_note_activity(client_ctx_t *ctx) {
+  uint64_t app = http2_session_app_activity(ctx->h2);
+  if (app != ctx->app_activity) {
+    ctx->app_activity = app;
+    ctx->last_app_activity_ms = now_ms();
+  }
+}
+
 int setup_client_session_async(io_loop_t *loop, int client_fd) {
   int flags = fcntl(client_fd, F_GETFL, 0);
   if (flags < 0) {
@@ -373,6 +457,25 @@
     return -1;
   }
 
+  uint8_t *plain_buffer = NULL;
+  tls_conn_t *tls = NULL;
+  if (g_tls_config != NULL) {
+    plain_buffer = malloc(read_size);
+    tls = tls_conn_new(g_tls_config, client_fd);
+    if (plain_buffer == NULL || tls == NULL) {
+      tls_conn_free(tls);
+      free(plain_buffer);
+      free(read_buffer);
+      task_free(task);
+      free(ctx);
+      close(client_fd);
+      return -1;
+    }
+  }
+  ctx->tls = tls;
+  ctx->plain_buffer = plain_buffer;
+  ctx->plain_size = read_size;
+
   task->fd = client_fd;
   task->state = TASK_ACTIVE;
   task->read_size = read_size;
@@ -391,6 +494,8 @@
   ctx->last_app_activity_ms = ctx->last_activity_ms;
 
   if (io_loop_add_task(loop, task) != 0) {
+    tls_conn_free(ctx->tls);
+    free(ctx->plain_buffer);
     free(read_buffer);
     task_free(task);
     free(ctx);
@@ -400,6 +505,8 @@
     task->context = NULL;
     task->on_complete = NULL;
     task_cancel(task);
+    tls_conn_free(ctx->tls);
+    free(ctx->plain_buffer);
     free(read_buffer);
     free(ctx);
     return -1;
@@ -426,8 +533,49 @@
     return;
   }
 
+  if (ctx->tls != NULL) {
+    if (client_tls_feed_encrypted(ctx, task) != 0) {
+      task->state = TASK_COMPLETED;
+      return;
+    }
+    if (!ctx->tls_ready) {
+      int hs = tls_conn_handshake(ctx->tls);
+      if (hs < 0) {
+        task->state = TASK_COMPLETED;
+        return;
+      }
+      if (client_tls_flush(ctx) != 0) {
+        task->state = TASK_COMPLETED;
+        return;
+      }
+      if (!hs) {
+        if (task->read_closed) {
+          task->state = tls_conn_pending_out(ctx->tls) != 0
+                            ? TASK_WAIT_WRITE
+                            : TASK_COMPLETED;
+        } else {
+          task->state = tls_conn_pending_out(ctx->tls) != 0
+                            ? TASK_WAIT_WRITE
+                            : TASK_ACTIVE;
+        }
+        return;
+      }
+      ctx->tls_ready = 1;
+    }
+  }
+
   if (ctx->h2 == NULL) {
-    ctx->h2 = http2_session_new_ex(task->fd, g_initial_window, g_conn_window);
+    if (ctx->tls != NULL) {
+      http2_transport_t transport = {
+          .ctx = ctx->tls,
+          .write = client_tls_write,
+          .writev = client_tls_writev,
+      };
+      ctx->h2 = http2_session_new_transport(&transport, task->fd,
+                                            g_stream_window, g_connection_window);
+    } else {
+      ctx->h2 = http2_session_new(task->fd, g_stream_window, g_connection_window);
+    }
     if (ctx->h2 == NULL) {
       task->state = TASK_COMPLETED;
       return;
@@ -435,7 +583,13 @@
     http2_session_set_unary_budget(ctx->h2, g_unary_budget);
   }
 
-  if (task->bytes_buffered > 0) {
+  if (ctx->tls != NULL) {
+    if (client_tls_read_plain(ctx) != 0) {
+      task->state = TASK_COMPLETED;
+      return;
+    }
+    client_note_activity(ctx);
+  } else if (task->bytes_buffered > 0) {
     ctx->last_activity_ms = now_ms();
     size_t buffered = task->bytes_buffered;
     size_t span = task_read_span(task);
@@ -446,11 +600,7 @@
       task->state = TASK_COMPLETED;
       return;
     }
-    uint64_t app = http2_session_app_activity(ctx->h2);
-    if (app != ctx->app_activity) {
-      ctx->app_activity = app;
-      ctx->last_app_activity_ms = now_ms();
-    }
+    client_note_activity(ctx);
     task_read_consume(task, buffered);
   }
 
@@ -458,9 +608,16 @@
     task->state = TASK_COMPLETED;
     return;
   }
+  if (ctx->tls != NULL && client_tls_flush(ctx) != 0) {
+    task->state = TASK_COMPLETED;
+    return;
+  }
 
   nghttp2_session *h = http2_raw(ctx->h2);
   int want_write = h != NULL && nghttp2_session_want_write(h) != 0;
+  if (ctx->tls != NULL && tls_conn_pending_out(ctx->tls) != 0) {
+    want_write = 1;
+  }
 
   if (task->read_closed) {
     // The peer cannot send more input. Flush what is already queued, then
@@ -484,6 +641,12 @@
     if (ctx->h2 != NULL) {
       http2_session_free(ctx->h2);
     }
+    if (ctx->tls != NULL) {
+      tls_conn_shutdown(ctx->tls);
+      tls_conn_flush(ctx->tls);
+    }
+    tls_conn_free(ctx->tls);
+    free(ctx->plain_buffer);
     client_unlink(ctx);
   }
   free(read_buffer);
```

## Full diff — `src/server/server.c`

```diff
--- a/src/server/server.c
+++ b/src/server/server.c
@@ -144,6 +144,13 @@
 // Initialize Server
 void Server_Init(FixedString5 *port) {
   server_limits_init();
+  if (g_stream_window == 0 || g_connection_window == 0) {
+    log_error("[*] H2_INITIAL_WINDOW_SIZE and H2_CONN_WINDOW_SIZE must be nonzero");
+    return;
+  }
+  if (server_tls_init() < 0) {
+    return;
+  }
   log_info("[*] Server listening on port %s", port->data);
 
   // Block SIGINT/SIGTERM in every thread (workers inherit the mask) and wait
@@ -173,4 +180,5 @@
   for (int i = 0; i < THREAD_COUNT; i++) {
     pthread_join(threads[i], NULL);
   }
+  server_tls_free();
 }
```

`server_tls_init()` runs after `server_limits_init()` and before workers start.
A negative return aborts startup. `server_tls_free()` runs after all workers have
joined, releasing the shared `SSL_CTX`.

## HTTP/2 transport interface diff

These changes are outside `src/server` but are required for any transport,
including TLS.

### `src/http2/http2.c`

```diff
--- a/src/http2/http2.c
+++ b/src/http2/http2.c
@@ -154,13 +154,16 @@
   http2_stream_t **slots; /* open-addressing id->stream index (power-of-two) */
   uint32_t slot_cap;      /* slot array length; 0 until the first stream */
   uint32_t slot_used;     /* live entries */
-  uint32_t initial_window; /* per-stream local receive window; 0 = default */
+  uint32_t stream_window; /* per-stream local receive window target */
   size_t unary_budget;    /* per-session cap; 0 = unlimited */
   size_t unary_bytes;     /* charged unary body bytes */
   uint64_t app_activity;  /* monotonic inbound request HEADERS/DATA count */
   int ping_ack_seen;
   uint64_t ping_ack_opaque;
   h2_parked_t parked;
+  http2_write_fn write_fn;    /* NULL = direct write(fd, ...) */
+  http2_writev_fn writev_fn;  /* NULL = direct writev(fd, ...) */
+  void *transport_ctx;
 };
 
 /* Sessions are worker-pinned; nghttp2 itself is not thread-safe. Enforce the
@@ -675,7 +678,9 @@
     if (off == length) {
       break;
     }
-    ssize_t w = write(s->fd, data + off, length - off);
+    ssize_t w = s->write_fn != NULL
+                    ? s->write_fn(s->transport_ctx, data + off, length - off)
+                    : write(s->fd, data + off, length - off);
     if (w > 0) {
       off += (size_t)w;
       continue;
@@ -825,7 +830,9 @@
     if (n == 0) {
       break; /* fully accounted (covers zero-length payloads) */
     }
-    ssize_t w = writev(s->fd, iov, n);
+    ssize_t w = s->writev_fn != NULL
+                    ? s->writev_fn(s->transport_ctx, iov, n)
+                    : writev(s->fd, iov, n);
     if (w < 0) {
       if (errno == EAGAIN || errno == EWOULDBLOCK) {
         return NGHTTP2_ERR_WOULDBLOCK; /* parked offsets kept; retried */
@@ -1329,7 +1336,7 @@
   if (frame->hd.stream_id != 0 && frame->hd.type == NGHTTP2_HEADERS &&
       frame->headers.cat == NGHTTP2_HCAT_REQUEST) {
     const http2_route_t *r = st != NULL ? st->route : NULL;
-    uint32_t win = s->initial_window;
+    uint32_t win = s->stream_window;
     if (r != NULL && r->initial_window != 0) {
       win = r->initial_window;
     }
@@ -1393,14 +1400,17 @@
   return 0;
 }
 
-http2_session_t *http2_session_new_ex(int client_fd,
-                                      uint32_t initial_window_size,
-                                      uint32_t conn_window_size) {
-  if (initial_window_size > (uint32_t)NGHTTP2_MAX_WINDOW_SIZE) {
-    initial_window_size = (uint32_t)NGHTTP2_MAX_WINDOW_SIZE;
+static http2_session_t *http2_session_new_impl(
+    const http2_transport_t *transport, int client_fd,
+    uint32_t stream_window_size, uint32_t connection_window_size) {
+  if (stream_window_size == 0 || connection_window_size == 0) {
+    return NULL;
+  }
+  if (stream_window_size > (uint32_t)NGHTTP2_MAX_WINDOW_SIZE) {
+    stream_window_size = (uint32_t)NGHTTP2_MAX_WINDOW_SIZE;
   }
-  if (conn_window_size > (uint32_t)NGHTTP2_MAX_WINDOW_SIZE) {
-    conn_window_size = (uint32_t)NGHTTP2_MAX_WINDOW_SIZE;
+  if (connection_window_size > (uint32_t)NGHTTP2_MAX_WINDOW_SIZE) {
+    connection_window_size = (uint32_t)NGHTTP2_MAX_WINDOW_SIZE;
   }
 
   http2_session_t *s = calloc(1, sizeof(*s));
@@ -1409,7 +1419,12 @@
   }
   s->fd = client_fd;
   s->owner = pthread_self();
-  s->initial_window = initial_window_size;
+  s->stream_window = stream_window_size;
+  if (transport != NULL) {
+    s->write_fn = transport->write;
+    s->writev_fn = transport->writev;
+    s->transport_ctx = transport->ctx;
+  }
 
   nghttp2_session_callbacks *cbs = NULL;
   if (nghttp2_session_callbacks_new(&cbs) != 0) {
@@ -1448,7 +1463,7 @@
 
   /* Emit the server connection preface (SETTINGS) right away, advertising
    * sane bounds: a concurrency cap and a header-list cap. The receive windows
-   * are raised separately (see http2_session_new_ex). */
+   * are raised separately (see http2_session_new). */
   const nghttp2_settings_entry sv[] = {
       {NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, 128},
       {NGHTTP2_SETTINGS_MAX_HEADER_LIST_SIZE, 65536},
@@ -1461,9 +1476,9 @@
   }
   /* Raise the connection-level receive window too; the peer sees a WINDOW_UPDATE
    * alongside the SETTINGS. */
-  if (conn_window_size > 0 &&
-      nghttp2_session_set_local_window_size(s->session, NGHTTP2_FLAG_NONE, 0,
-                                            (int32_t)conn_window_size) != 0) {
+  if (nghttp2_session_set_local_window_size(
+          s->session, NGHTTP2_FLAG_NONE, 0,
+          (int32_t)connection_window_size) != 0) {
     nghttp2_session_del(s->session);
     free(s);
     return NULL;
@@ -1476,10 +1491,26 @@
   return s;
 }
 
-http2_session_t *http2_session_new(int client_fd) {
-  return http2_session_new_ex(client_fd, 0, 0);
+http2_session_t *http2_session_new(int client_fd,
+                                   uint32_t stream_window_size,
+                                   uint32_t connection_window_size) {
+  return http2_session_new_impl(NULL, client_fd, stream_window_size,
+                                connection_window_size);
+}
+
+http2_session_t *http2_session_new_transport(const http2_transport_t *transport,
+                                             int client_fd,
+                                             uint32_t stream_window_size,
+                                             uint32_t connection_window_size) {
+  if (transport == NULL || transport->write == NULL ||
+      transport->writev == NULL) {
+    return NULL;
+  }
+  return http2_session_new_impl(transport, client_fd, stream_window_size,
+                                connection_window_size);
 }
 
+
 int http2_session_feed(http2_session_t *s, const uint8_t *data, size_t len) {
   if (s == NULL) {
     return -1;
```

The two write call sites now prefer the transport callback and fall back to the
original `write()`/`writev()` when no transport was installed. The constructor
body was renamed to a private implementation; the old public constructor remains
as a thin wrapper, so existing callers are unaffected.

### `include/http2_server.h`

```diff
--- a/include/http2_server.h
+++ b/include/http2_server.h
@@ -40,9 +40,13 @@
 #include <nghttp2/nghttp2.h>
 #include <stddef.h>
 #include <stdint.h>
+#include <sys/types.h>
+
+struct iovec;
 
 /* Caps against unbounded peer-driven growth. Tweak as needed. */
 enum {
+  HTTP2_DEFAULT_WINDOW_SIZE = 65535,        /* HTTP/2 initial receive window */
   HTTP2_MAX_HEADERS = 128,                  /* header fields stored per stream */
   HTTP2_MAX_BODY = 4u * 1024u * 1024u,     /* unary body buffered per stream */
   HTTP2_MAX_RESIDUAL = 1u * 1024u * 1024u, /* streaming: incomplete-tail cap */
@@ -176,7 +180,7 @@
     http2_stream_fn stream;         /* handler when mode == HTTP2_STREAMING */
   } handler;
   /* Optional per-route stream receive window, applied with a WINDOW_UPDATE at
-   * request HEADERS. 0 keeps the session default from http2_session_new_ex(). */
+   * request HEADERS. 0 keeps the session default from http2_session_new(). */
   uint32_t initial_window;
 } http2_route_t;
 
@@ -201,28 +205,54 @@
 const http2_header_t *http2_stream_headers(const http2_stream_t *st);
 
 
-/* Create a server session bound to `client_fd` (must be non-blocking).
- * Emits the server connection preface (SETTINGS). Returns NULL on failure.
+/* Optional write transport. The callbacks must not block. Return the number
+ * of bytes accepted, 0 if the transport cannot accept more now, or a negative
+ * value on fatal error. `writev` receives the same scatter/gather buffers the
+ * plaintext path would pass to writev(). A transport is copied into the
+ * session; its context must outlive the session.
  */
-http2_session_t *http2_session_new(int client_fd);
+typedef ssize_t (*http2_write_fn)(void *ctx, const uint8_t *buf, size_t len);
+typedef ssize_t (*http2_writev_fn)(void *ctx, const struct iovec *iov,
+                                  int iovcnt);
+
+typedef struct http2_transport {
+  void *ctx;
+  http2_write_fn write;
+  http2_writev_fn writev;
+} http2_transport_t;
 
-/* Tuned variant of http2_session_new(). `initial_window_size` raises each
- * stream's local receive window at request HEADERS (one WINDOW_UPDATE per
- * stream); `conn_window_size` raises the connection-level receive window (one
- * WINDOW_UPDATE per connection). Both are in bytes; pass 0 to keep nghttp2's
- * default (65535). Values above NGHTTP2_MAX_WINDOW_SIZE are clamped.
+/* Create a server session bound to `client_fd` (must be non-blocking).
+ * `stream_window_size` raises each stream's local receive window at request
+ * HEADERS (one WINDOW_UPDATE per stream); `connection_window_size` raises the
+ * connection-level receive window (one WINDOW_UPDATE per connection). Both are
+ * in bytes and must be nonzero; use HTTP2_DEFAULT_WINDOW_SIZE for the standard
+ * 65535-byte window. Values above NGHTTP2_MAX_WINDOW_SIZE are clamped, but
+ * zero is rejected.
  *
  * The stream window is applied with WINDOW_UPDATE rather than advertised via
  * SETTINGS_INITIAL_WINDOW_SIZE: nghttp2 does not mirror that setting into its
  * local window, so advertising it here as well would double the peer's send
  * window relative to what the module enforces. Larger windows raise throughput
- * on high-BDP links; the one-per-stream WINDOW_UPDATE is why
- * initial_window_size defaults to 0 at the server layer. The module's
- * per-stream body/residual caps still bound memory. Individual routes may
- * override the stream window with http2_route_t.initial_window. */
-http2_session_t *http2_session_new_ex(int client_fd,
-                                      uint32_t initial_window_size,
-                                      uint32_t conn_window_size);
+ * on high-BDP links; the one-per-stream WINDOW_UPDATE is why the server layer
+ * passes 0 by default. The module's per-stream body/residual caps still bound
+ * memory. Individual routes may override the stream window with
+ * http2_route_t.initial_window.
+ *
+ * Emits the server connection preface (SETTINGS). Returns NULL on failure.
+ */
+http2_session_t *http2_session_new(int client_fd,
+                                   uint32_t stream_window_size,
+                                   uint32_t connection_window_size);
+
+/* General-purpose transport variant. The response API is unchanged: responses
+ * are written through `transport` instead of directly to `client_fd`. Pass a
+ * TLS writer for encrypted output, or use http2_session_new() for the original
+ * plaintext direct-fd path. Returns NULL for an invalid transport.
+ */
+http2_session_t *http2_session_new_transport(const http2_transport_t *transport,
+                                             int client_fd,
+                                             uint32_t stream_window_size,
+                                             uint32_t connection_window_size);
 
 /* Process-wide cap on unary request-body buffers held across all sessions.
  * 0 = unlimited (the default). When a new chunk would exceed it the offending
```

### `Makefile`

```diff
--- a/Makefile
+++ b/Makefile
@@ -13,7 +13,9 @@
 NGHTTP2_LIBS := $(shell pkg-config --libs libnghttp2 2>/dev/null)
 URING_CFLAGS := $(shell pkg-config --cflags liburing 2>/dev/null)
 URING_LIBS := $(shell pkg-config --libs liburing 2>/dev/null)
-CFLAGS += $(NGHTTP2_CFLAGS) $(URING_CFLAGS)
+SSL_CFLAGS := $(shell pkg-config --cflags openssl 2>/dev/null)
+SSL_LIBS := $(shell pkg-config --libs openssl 2>/dev/null)
+CFLAGS += $(NGHTTP2_CFLAGS) $(URING_CFLAGS) $(SSL_CFLAGS)
 
 # -----------------------------
 # Directories
@@ -111,7 +113,7 @@
 # Ensure the final executable waits for dependency resolution FIRST
 $(TARGET): $(LIB_DIR)/.excal.stamp $(MAIN_OBJ) $(APP_OBJS) $(STATIC_LIB_FILES)
 	@mkdir -p $(BIN_DIR)
-	$(CC) $(CFLAGS) $(MAIN_OBJ) $(APP_OBJS) $(STATIC_LIB_FILES) $$(find $(LIB_DIR) -maxdepth 1 -name '*.a' 2>/dev/null) $(NGHTTP2_LIBS) $(URING_LIBS) -o $@
+	$(CC) $(CFLAGS) $(MAIN_OBJ) $(APP_OBJS) $(STATIC_LIB_FILES) $$(find $(LIB_DIR) -maxdepth 1 -name '*.a' 2>/dev/null) $(NGHTTP2_LIBS) $(URING_LIBS) $(SSL_LIBS) -o $@
 
 all: $(TARGET)
```

## New TLS module

`include/tls_server.h` exposes an opaque `tls_config_t` and `tls_conn_t` with:

- config creation/free and certificate/key loading;
- handshake driving with ALPN `h2` validation;
- encrypted-input feeding and plaintext reads;
- plaintext writes and `writev`-style writes;
- encrypted-output flushing and pending-byte inspection;
- shutdown and EOF/wants-read queries.

`src/tls_server/tls_server.c` implements this with OpenSSL memory BIOs. It keeps encrypted
output in a bounded 4 MiB buffer. `tls_conn_write()` returns 0 when that buffer is
full, which becomes HTTP/2 `WOULDBLOCK`; the server then waits for writability and
flushes. This provides backpressure without changing the HTTP/2 response API.

## Configuration and behavior

- No `H2_TLS_CERT` and no `H2_TLS_KEY`: plaintext h2c prior-knowledge mode.
- Both set: every accepted connection performs TLS with ALPN `h2`.
- Only one set, or certificate/key load failure: startup fails.
- Plaintext and TLS use the same HTTP/2 session code and response functions.
- The existing plaintext smoke path remains available.

Example:

```bash
# Plaintext h2c prior knowledge
./bin/program

# TLS with ALPN h2
H2_TLS_CERT=/path/cert.pem H2_TLS_KEY=/path/key.pem ./bin/program

# TLS client smoke test
curl --http2 -k https://127.0.0.1:8000/3/7 --data-binary @request.bin
```

## Verification performed

- Clean ASan build.
- Plaintext HTTP/2 smoke test on epoll and io_uring.
- TLS HTTP/2 smoke test on epoll and io_uring using a generated self-signed
  certificate and `curl --http2 -k`.
- 20,469-byte response matched byte-for-byte in all four tests.

## The Cost of this TLS implementation

This section covers the full data-path cost of the current memory-BIO TLS
transport: copies, OpenSSL work, crypto, memory, handshakes, and the measured
effect on this server. It also ranks possible optimizations by impact, complexity,
and feasibility.

### 1. User-space copies

For a unary HTTP/2 request/response, the relevant paths are:

```text
inbound:
  kernel -> async read ring                 baseline
  read ring -> tls_conn_feed -> BIO         +1 ciphertext copy
  SSL_read -> plain_buffer                  +1 decrypted plaintext copy
  plain_buffer -> http2 unary body          baseline (streaming can avoid this)

outbound:
  response payload -> HTTP/2 queue          baseline
  HTTP/2 -> SSL_write -> BIO                +1 encryption/output copy
  BIO -> kernel write                       baseline (direct from BIO memory)
```

So this implementation adds roughly **two user-space copies on inbound** and
**one on outbound**, plus the baseline kernel copies and HTTP/2 assembly copies.
On a partial socket write, the module keeps an offset into the BIO rather than
copying the remainder into a separate staging buffer.

Approximate cost of one copy at common server bandwidth:

| Bytes | One copy at 20 GB/s | One copy at 40 GB/s |
|---:|---:|---:|
| 1 KiB | ~50 ns | ~25 ns |
| 16 KiB | ~0.8 µs | ~0.4 µs |
| 64 KiB | ~3.2 µs | ~1.6 µs |
| 1 MiB | ~52 µs | ~26 µs |

These numbers are memory-bandwidth estimates, not full TLS costs. In the
measured end-to-end test, the extra copies were a small fraction of the total
TLS overhead; crypto and OpenSSL record processing dominated.

The memory BIO staging copy is avoidable in principle. The plaintext output copy
is effectively required by the normal OpenSSL `SSL_read()` API.

### 2. OpenSSL overhead

OpenSSL adds work beyond the raw cipher:

- TLS record framing, AEAD tags, sequence numbers, and record-state updates.
- `BIO_write()`/`BIO_read()` indirection and the memory-BIO copies above.
- `SSL_read()`/`SSL_write()` state-machine work and error checks.
- Per-connection OpenSSL objects and record buffers.
- Handshake processing on every new connection.
- A separate `write()` syscall path for encrypted output from
  `tls_conn_flush()`.

OpenSSL's write BIO is the encrypted-output buffer and is capped at 4 MiB per
connection. It only reaches that size if the socket is blocked while the
application keeps producing output; normal traffic keeps it much smaller.
`tls_conn_flush()` writes directly from the BIO's memory and tracks a consumed
offset, so a partial write does not require a separate remainder copy. Each TLS
connection also owns a 16 KiB plaintext staging buffer in addition to the
existing 16 KiB async read buffer and OpenSSL's own state.

The current server does not do explicit TLS session-cache or ticket tuning.
Handshake cost is therefore paid on new connections, with whatever resumption
OpenSSL provides by default. That cost is amortized in the benchmark below
because many requests reuse connections, but it matters for connection churn.

### 3. Crypto overhead

The symmetric cipher is the main per-byte cost:

- AES-GCM is typically the fastest choice on x86 with AES-NI.
- ChaCha20-Poly1305 is usually slower on CPUs with AES-NI, but can win on
  CPUs without it.
- The negotiated cipher depends on the client and OpenSSL policy.
- The RSA certificate affects the handshake, not bulk record encryption.
- TLS 1.3 and TLS 1.2 have similar bulk-crypto costs; TLS 1.3 has a cheaper
  handshake and fewer round trips.

Small TLS records cost disproportionately more because per-record overhead is
paid per record. Larger records improve throughput but are bounded by the TLS
maximum fragment size and the HTTP/2 frame size used by the caller.

### 4. Measured local result

Local loopback, four server workers, epoll, `curl --parallel` with HTTP/2
multiplexing, 64 max parallel, median of five runs, server logging disabled:

| Payload | Plain req/s | TLS req/s | Throughput change | Plain CPU/req | TLS CPU/req |
|---:|---:|---:|---:|---:|---:|
| 1 KiB × 20k | 16,035 | 15,250 | −4.9% | 0.07 ms | 0.10 ms |
| 16 KiB × 5k | 13,374 | 11,224 | −16.1% | 0.12 ms | 0.16 ms |
| 64 KiB × 2k | 4,638 | 3,630 | −21.7% | 0.37 ms | 0.49 ms |

Server CPU increased by roughly 35–53%. This is a local synthetic test with a
user-space curl client; absolute numbers are not production capacity. The 64 KiB
plaintext result is also lower than the 16 KiB result because of client,
concurrency, and loopback effects, so compare each row's plain and TLS columns,
not rows against each other.

The benchmark above was taken before the direct-BIO flush optimization. That
change removes one outbound staging copy; the expected gain is small compared
with crypto, and local reruns were noisy enough that the table is left as the
pre-optimization baseline.

The important conclusion is that TLS costs grow with payload size, but the
staging copies are not the main cost. At 64 KiB, two extra copies are on the order
of a few microseconds, while the measured TLS CPU increase was about 120 µs per
request.

### 5. Speedup options

| Option | What it removes | Expected impact | Complexity | Feasibility |
|---|---|---|---|---|
| Reuse connections; tune session resumption/tickets | Repeated handshakes | High for short-lived/small RPCs | Low–medium | Feasible now |
| Use streaming routes for large payloads | HTTP/2 unary body copy | Medium–high for large payloads | Low | Already supported |
| Direct BIO-to-socket flush with partial-write offset | One outbound staging copy | Low–medium | Implemented | Done |
| `SSL_set_fd()`/socket BIO | One inbound BIO staging copy | Low–medium | High: moves socket reads out of async engine | Possible but architectural |
| Custom zero-copy BIO over read ring | One inbound BIO copy | Low | High | Possible; OpenSSL may still copy internally |
| kTLS (`SSL_OP_ENABLE_KTLS`) | OpenSSL user-space crypto and copies | High for large payloads/high throughput | High, platform-specific | Optional Linux fast path |
| In-place decryption | Plaintext output copy | Low–medium | Very high; OpenSSL public API does not support it | Not practical here |
| Larger read/record sizes | Per-record overhead | Low–medium | Medium | Depends on TLS/HTTP2 limits |
| Reuse preallocated buffers | Allocation and first-touch cost | Low | Low | Feasible |

Ranked recommendation:

1. **Do now:** keep connections alive and tune/rely on TLS session resumption.
   Handshake work is more expensive than the staging copies for small RPCs.
2. **Do for large payloads:** use streaming HTTP/2 routes to avoid the unary body
   copy. This is independent of TLS and already available.
3. **Already implemented:** flush directly from the BIO and track a partial-write
   offset. This removes the separate outbound staging copy.
4. **Optional:** add kTLS as a fast path when both TX and RX are available, with
   automatic fallback to the current path.
5. **Do not pursue now:** custom zero-copy BIOs, in-place decryption, or an
   `SSL_set_fd()` refactor. Their complexity is high and their incremental gain
   over the current design is small relative to crypto and OpenSSL overhead.
