> **Historical document.** This file describes an earlier architecture and API.
> For the current server, use `include/http2_server.h`, `include/http_exchange.h`,
> and the `src/http_mw_*` middleware modules.

# The HTTP/2 Library, From Scratch

This is the HTTP/2 companion to the async-engine tutorial. It builds the neutral
HTTP/2 server layer in `include/http2_server.h` and `src/http2/http2.c` one decision at
a time.

The destination is a library, not a `main()` program:

- one `http2_session_t` per connection;
- a route table owned by the application;
- request handlers that receive whole messages;
- response helpers that queue output without blocking the worker;
- a public header that hides the nghttp2 callbacks and the wire bookkeeping.

We will get there by adding capabilities in the order a real server needs them.
Some steps will be replaced later. That is intentional. The phases are not a
vertical slice of the final files; they are the sequence of refactors that
produces those files.

## Phase 0 — What are we actually building?

Start with the boundary.

nghttp2 already knows HTTP/2 framing and HPACK. It does not know our sockets,
our event loop, our routes, our body limits, or our reply lifetimes. If we hand
it a socket and let it do blocking I/O, it will fight the async engine. If we
let it copy every byte in both directions, we lose the zero-copy design.

So the layer has four jobs:

1. **Move bytes.** Take nonblocking reads from the async engine, call
   `nghttp2_session_mem_recv2()`, and let nghttp2 pull framed output through
   callbacks that write to the socket.
2. **Turn frames into requests.** Store request headers, associate them with a
   stream, resolve a route, and assemble the body according to the route's
   mode.
3. **Turn replies into frames.** Queue response payloads, expose a small
   `respond_*` API, and let nghttp2 frame them.
4. **Own the lifetime.** Enforce body caps, flow-control credit, per-stream
   state, graceful drain, and one-worker-thread ownership.

The initial scope is deliberately narrow:

- HTTP/2 over cleartext with prior knowledge (`h2c`);
- server sessions only;
- one session per connection, driven by one worker thread;
- application routes registered before serving;
- unary requests and streaming requests;
- no HTTP/1.1 upgrade, no TLS, no server push, no priority policy.

At this point we have no final structs and no final public API. We have a
question: what is the smallest thing that can speak HTTP/2?

## Phase 1 — Make one connection speak HTTP/2

### Step 1.1 — Wrap the nghttp2 session

The first type is not `http2_session_t`. It is a temporary connection object
that owns the nghttp2 session and the file descriptor:

```c
// src/http2/http2.c
struct h2_conn {
  nghttp2_session *ng;
  int fd;
};
```

nghttp2 needs callbacks before it can be created. The first callback is the
send callback: nghttp2 hands us framed bytes, and we write them to the
nonblocking socket.

```c
static ssize_t on_send(nghttp2_session *ng, const uint8_t *data,
                       size_t length, int flags, void *user_data) {
  (void)ng;
  (void)flags;

  struct h2_conn *c = user_data;
  size_t off = 0;

  while (off < length) {
    ssize_t n = write(c->fd, data + off, length - off);

    if (n > 0) {
      off += (size_t)n;
      continue;
    }

    if (n < 0 && errno == EINTR) {
      continue;
    }

    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      break;
    }

    return NGHTTP2_ERR_CALLBACK_FAILURE;
  }

  if (off == 0) {
    return NGHTTP2_ERR_WOULDBLOCK;
  }

  return (ssize_t)off;
}
```

This build of nghttp2 wants v2 (bytes-sent) semantics: return the number of
bytes written, not `0` on success. Returning `0` after writing means “sent
nothing”, and nghttp2 will keep asking for the same frame. If nothing could be
written, return `NGHTTP2_ERR_WOULDBLOCK`.

Create the session as a server session, submit the initial SETTINGS, and send
the server preface:

```c
struct h2_conn *h2_conn_new(int fd) {
  struct h2_conn *c = calloc(1, sizeof(*c));
  if (c == NULL) {
    return NULL;
  }
  c->fd = fd;

  nghttp2_session_callbacks *cbs = NULL;
  if (nghttp2_session_callbacks_new(&cbs) != 0) {
    free(c);
    return NULL;
  }

  nghttp2_session_callbacks_set_send_callback(cbs, on_send);

  if (nghttp2_session_server_new2(&c->ng, cbs, c, NULL) != 0) {
    nghttp2_session_callbacks_del(cbs);
    free(c);
    return NULL;
  }
  nghttp2_session_callbacks_del(cbs);

  const nghttp2_settings_entry settings[] = {
      {NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, 128},
      {NGHTTP2_SETTINGS_MAX_HEADER_LIST_SIZE, 65536},
  };

  if (nghttp2_submit_settings(c->ng, NGHTTP2_FLAG_NONE, settings,
                              sizeof(settings) / sizeof(settings[0])) != 0 ||
      nghttp2_session_send(c->ng) != 0) {
    nghttp2_session_del(c->ng);
    free(c);
    return NULL;
  }

  return c;
}
```

The first module surface is small:

```c
typedef struct http2_session http2_session_t;

http2_session_t *http2_session_new(int fd);
void http2_session_free(http2_session_t *s);
```

We have not exposed `h2_conn`, nghttp2, or the send callback. The caller only
knows that it can create and destroy a session.

### Step 1.2 — Feed inbound bytes

The async engine reads bytes. The HTTP/2 layer must consume exactly those bytes
and flush anything nghttp2 queued as a result:

```c
int http2_session_feed(http2_session_t *s, const uint8_t *data, size_t len) {
  nghttp2_ssize consumed =
      nghttp2_session_mem_recv2(s->ng, data, len);

  if (consumed < 0) {
    return -1;
  }

  if (nghttp2_session_want_write(s->ng) &&
      nghttp2_session_send(s->ng) != 0) {
    return -1;
  }

  return 0;
}
```

`mem_recv2()` may invoke callbacks and may queue control frames. The trailing
`session_send()` is what pushes SETTINGS acknowledgements, PING ACKs, and
response bytes that were produced while processing the input.

Callers must treat `-1` as “this connection is finished”; there is no partial
HTTP/2 recovery at this layer.

### Step 1.3 — See a request

nghttp2 reports header fields one at a time. A first observer just prints them:

```c
static int on_header(nghttp2_session *ng, const nghttp2_frame *frame,
                     const uint8_t *name, size_t namelen,
                     const uint8_t *value, size_t valuelen,
                     uint8_t flags, void *user_data) {
  (void)ng;
  (void)flags;
  (void)user_data;

  if (frame->hd.type == NGHTTP2_HEADERS) {
    printf("%.*s: %.*s\n", (int)namelen, name, (int)valuelen, value);
  }
  return 0;
}
```

Header names and values are binary and are not NUL-terminated. Never use
`strlen()` or `strcmp()` on them. Use `namelen` and `valuelen`.

At the end of a request, `on_frame_recv` sees END_STREAM. For the first
experiment, answer every request with a fixed body:

```c
static ssize_t read_hello(nghttp2_session *ng, int32_t sid, uint8_t *buf,
                          size_t length, uint32_t *flags,
                          nghttp2_data_source *source, void *user_data) {
  (void)ng;
  (void)sid;
  (void)source;
  (void)user_data;

  static const char body[] = "hello\n";
  size_t n = length < sizeof(body) - 1 ? length : sizeof(body) - 1;
  memcpy(buf, body, n);
  if (n == sizeof(body) - 1) {
    *flags |= NGHTTP2_DATA_FLAG_EOF;
  }
  return (ssize_t)n;
}

static int on_frame_recv(nghttp2_session *ng, const nghttp2_frame *frame,
                         void *user_data) {
  (void)user_data;

  if (frame->hd.type == NGHTTP2_HEADERS &&
      (frame->hd.flags & NGHTTP2_FLAG_END_STREAM)) {
    nghttp2_data_provider provider = {
        .source.ptr = NULL,
        .read_callback = read_hello,
    };
    nghttp2_nv response[] = {
        {(uint8_t *)":status", (uint8_t *)"200", 7, 3, NGHTTP2_NV_FLAG_NONE},
    };

    nghttp2_submit_response(ng, frame->hd.stream_id, response, 1, &provider);
  }
  return 0;
}
```

This is enough to prove the connection works. It is not yet a library. Every
request gets the same answer, nothing is routed, no body is read, and the reply
provider is a global function with no per-request state.

The public surface at the end of Phase 1:

```c
typedef struct http2_session http2_session_t;

http2_session_t *http2_session_new(int fd);
int http2_session_feed(http2_session_t *s, const uint8_t *data, size_t len);
void http2_session_free(http2_session_t *s);
```

### Phase 1 pain log

1. **No request object.** Header fields are printed, not stored.
2. **No routing.** Every request receives the same response.
3. **No body.** DATA frames are ignored.
4. **No ownership.** There is no stream lifetime and no per-stream state.
5. **Blocking on partial writes.** The send callback does not park a partial
   frame; it assumes the socket accepts everything.
6. **No limits.** A peer can make us buffer or allocate without bound.
7. **No flow control policy.** nghttp2's automatic accounting is still doing
   whatever it does by default.
8. **No graceful drain.** There is no GOAWAY, no active-stream count, and no
   keepalive.

Phase 2 starts by giving requests a name and a lifetime.

## Phase 2 — Requests become streams

### Step 2.1 — Store headers

A real handler needs the request after `on_header` returns. Copy each field into
a stream-owned node. Use one allocation for the name and value:

```c
typedef struct h2_header {
  uint8_t *name;
  size_t namelen;
  uint8_t *value;
  size_t valuelen;
  struct h2_header *next;
} h2_header_t;

static int header_add(h2_stream_t *st, const uint8_t *name, size_t namelen,
                      const uint8_t *value, size_t valuelen) {
  if (st->header_count >= HTTP2_MAX_HEADERS) {
    return -1;
  }

  h2_header_t *h = calloc(1, sizeof(*h));
  if (h == NULL) {
    return -1;
  }

  size_t blen = namelen + valuelen;
  uint8_t *blk = malloc(blen > 0 ? blen : 1);
  if (blk == NULL) {
    free(h);
    return -1;
  }

  memcpy(blk, name, namelen);
  memcpy(blk + namelen, value, valuelen);

  h->name = blk;
  h->value = blk + namelen;
  h->namelen = namelen;
  h->valuelen = valuelen;

  if (st->headers_tail != NULL) {
    st->headers_tail->next = h;
  } else {
    st->headers = h;
  }
  st->headers_tail = h;
  st->header_count++;
  return 0;
}
```

The value aliases into the same block after the name, so the destructor frees
only `h->name` and then the node.

Keep arrival order. It matters for debugging, and HTTP/2 does not promise a
canonical order for arbitrary fields.

### Step 2.2 — Give streams a lifetime

The first registry is a singly linked list:

```c
typedef struct h2_stream {
  int32_t id;
  h2_header_t *headers;
  h2_header_t *headers_tail;
  size_t header_count;
  struct h2_stream *next;
} h2_stream_t;

struct http2_session {
  nghttp2_session *ng;
  int fd;
  h2_stream_t *streams;
};
```

Create a stream on the first HEADERS for its id, reuse it for later HEADERS
(trailers) and DATA, and free it on `on_stream_close`.

```c
static h2_stream_t *find_or_create_stream(http2_session_t *s, int32_t id) {
  for (h2_stream_t *st = s->streams; st != NULL; st = st->next) {
    if (st->id == id) {
      return st;
    }
  }

  h2_stream_t *st = calloc(1, sizeof(*st));
  if (st == NULL) {
    return NULL;
  }
  st->id = id;
  st->next = s->streams;
  s->streams = st;
  return st;
}

static void free_stream(http2_session_t *s, int32_t id) {
  h2_stream_t **link = &s->streams;

  while (*link != NULL) {
    h2_stream_t *st = *link;
    if (st->id != id) {
      link = &st->next;
      continue;
    }

    *link = st->next;

    h2_header_t *h = st->headers;
    while (h != NULL) {
      h2_header_t *next = h->next;
      free(h->name);
      free(h);
      h = next;
    }

    free(st);
    return;
  }
}
```

This is O(n) per lookup, which is fine while the design is still moving. We will
replace it with an id-keyed hash table after the lifecycle is stable.

### Step 2.3 — Route by `:path`

Headers are stored in arrival order. Look up `:path` with a length-aware
comparison:

```c
static const uint8_t *header_get(const h2_stream_t *st, const char *name,
                                 size_t namelen, size_t *vlen) {
  for (const h2_header_t *h = st->headers; h != NULL; h = h->next) {
    if (h->namelen == namelen &&
        memcmp(h->name, name, namelen) == 0) {
      if (vlen != NULL) {
        *vlen = h->valuelen;
      }
      return h->value;
    }
  }
  return NULL;
}
```

A route is application configuration. Start with the smallest useful form:

```c
typedef struct h2_route {
  const char *path;
  void (*unary)(http2_session_t *s, h2_stream_t *st,
                const uint8_t *body, size_t len);
} h2_route_t;
```

Store routes in a fixed table. A linear search is enough for the first version:

```c
#define H2_ROUTE_MAX 64

static h2_route_t g_routes[H2_ROUTE_MAX];
static size_t g_route_count;

int http2_route_add(const h2_route_t *r) {
  if (r == NULL || r->path == NULL || g_route_count == H2_ROUTE_MAX) {
    return -1;
  }
  g_routes[g_route_count++] = *r;
  return 0;
}

static const h2_route_t *route_lookup(const uint8_t *path, size_t plen) {
  for (size_t i = 0; i < g_route_count; i++) {
    if (strlen(g_routes[i].path) == plen &&
        memcmp(g_routes[i].path, path, plen) == 0) {
      return &g_routes[i];
    }
  }
  return NULL;
}
```

At request HEADERS, resolve the path and bind the handler to the stream. An
unknown path is rejected with RST_STREAM rather than left hanging:

```c
static void wire_request(http2_session_t *s, h2_stream_t *st,
                         const h2_route_t *r) {
  if (r == NULL) {
    nghttp2_submit_rst_stream(s->ng, NGHTTP2_FLAG_NONE, st->id,
                              NGHTTP2_PROTOCOL_ERROR);
    return;
  }
  st->handler_unary = r->unary;
}
```

The public surface grows:

```c
int http2_route_add(const http2_route_t *r);
```

The route table is global and must be registered before serving. That is a
deliberate simplification: one process, one route set. If the application needs
per-listener routes later, the table can move into a configuration object.

### Phase 2 pain log

1. **The body is still ignored.** A unary handler cannot run until END_STREAM.
2. **The route has only one mode.** There is no streaming handler.
3. **The registry is a list.** Lookup cost grows with concurrent streams.
4. **No per-stream application state.** The handler only gets the request.
5. **No flow-control policy.** Buffered bytes are not credited deliberately.
6. **No limits on header storage beyond the count.** Header-list size is still
   whatever nghttp2 accepted.

Phase 3 gives unary requests a body.

## Phase 3 — Read a unary body

### Step 3.1 — Accumulate DATA

nghttp2 delivers DATA chunks through `on_data_chunk_recv_callback`. For a unary
route, the layer buffers the whole body:

```c
typedef struct h2_stream {
  /* ... headers ... */
  uint8_t *body;
  size_t body_len;
  size_t body_cap;
  int mode; /* 0 = unary, 1 = streaming, for now */
} h2_stream_t;

static int body_reserve(h2_stream_t *st, size_t want) {
  if (want <= st->body_cap) {
    return 0;
  }

  size_t cap = st->body_cap != 0 ? st->body_cap : 256;
  while (cap < want) {
    if (cap > SIZE_MAX / 2) {
      cap = want;
      break;
    }
    cap *= 2;
  }

  uint8_t *p = realloc(st->body, cap);
  if (p == NULL) {
    return -1;
  }
  st->body = p;
  st->body_cap = cap;
  return 0;
}

static int on_data_chunk(nghttp2_session *ng, uint8_t flags, int32_t sid,
                         const uint8_t *data, size_t len, void *user_data) {
  (void)ng;
  (void)flags;

  http2_session_t *s = user_data;
  h2_stream_t *st = find_or_create_stream(s, sid);
  if (st == NULL) {
    return NGHTTP2_ERR_CALLBACK_FAILURE;
  }

  if (st->body_len + len > HTTP2_MAX_BODY) {
    return NGHTTP2_ERR_CALLBACK_FAILURE;
  }

  if (body_reserve(st, st->body_len + len) != 0) {
    return NGHTTP2_ERR_CALLBACK_FAILURE;
  }

  memcpy(st->body + st->body_len, data, len);
  st->body_len += len;
  return 0;
}
```

`HTTP2_MAX_BODY` bounds one unary stream. Memory still grows with concurrent
streams, so this is not enough; budgets come next.

### Step 3.2 — Dispatch exactly once

END_STREAM is visible in `on_frame_recv`. It can arrive on HEADERS (a bodyless
request) or on DATA. Dispatch the unary handler at that point:

```c
if ((frame->hd.type == NGHTTP2_HEADERS ||
     frame->hd.type == NGHTTP2_DATA) &&
    (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) &&
    frame->hd.stream_id != 0) {
  h2_stream_t *st = find_stream(s, frame->hd.stream_id);
  if (st != NULL && st->mode == 0 && st->handler_unary != NULL) {
    st->handler_unary(s, st, st->body, st->body_len);
  }
}
```

The handler is called synchronously from `http2_session_feed()`, on the worker
thread. A unary handler therefore sees the whole body and may respond
immediately, but it must not block.

### Step 3.3 — Optional framing validation

A route can supply a pure framing function. For unary mode it acts as a
validator: the buffered body must be exactly one complete message.

```c
typedef size_t (*h2_msg_framing_cb)(const void *cfg, const h2_view_t *v,
                                    size_t cur, size_t avail);
```

A two-segment view lets the same function work when a message straddles the
boundary between an old residual and a new chunk:

```c
typedef struct h2_view {
  const uint8_t *a;
  size_t an;
  const uint8_t *b;
  size_t bn;
} h2_view_t;
```

For unary mode, build a one-segment view and require `framing() == body_len`.
For streaming mode this same callback will decide where each message ends. The
important contract is that it is pure: the same bytes produce the same length,
and it never reads outside the available range.

### Step 3.4 — Charge body memory

Per-stream caps are not enough. Add a per-session budget and a process-wide
budget:

```c
struct http2_session {
  /* ... */
  size_t unary_budget; /* 0 = unlimited */
  size_t unary_bytes;
};

static _Atomic size_t g_unary_bytes;
static _Atomic size_t g_unary_budget; /* 0 = unlimited */
```

Charge before growing the buffer. The process budget uses a compare-and-swap so
concurrent workers cannot overshoot it; the session budget is owner-thread-only
and can be a plain field.

On stream close, release exactly what that stream charged. Keep a
`unary_charged` field on the stream so cleanup is idempotent even if allocation
fails midway.

### Phase 3 pain log

1. **Unary buffers the whole body.** A streaming request would use unbounded
   memory.
2. **No reply queue.** The handler still needs a blocking-free way to send more
   than one response message.
3. **No flow-control credit policy.** Bytes are buffered without deciding when
   to return credit.
4. **No trailers.** A response can only end with DATA.
5. **No per-route application state.** Handlers have nowhere to keep a parser,
   auth result, or producer handle across callbacks.
6. **The registry is still a list.**

Phase 4 builds the sending side.

## Phase 4 — Replies without blocking

### Step 4.1 — A small response API

Handlers should not call nghttp2 directly. Start with four operations:

```c
int http2_respond_begin(http2_session_t *s, int32_t sid, int status,
                        const nghttp2_nv *headers, size_t nheaders);
int http2_respond_msg(http2_session_t *s, int32_t sid,
                      const uint8_t *body, size_t len, int end_stream);
int http2_respond_end(http2_session_t *s, int32_t sid);
int http2_respond_trailers(http2_session_t *s, int32_t sid,
                           const nghttp2_nv *nva, size_t nvlen);
```

`respond_msg()` is the important one. The first call opens a 200 response if
`respond_begin()` was not used. Each call queues one payload. `end_stream` marks
the final payload.

`respond_begin()` mints `:status` itself from an integer and lets nghttp2 copy
the extra header fields. A stack status buffer is safe because nghttp2 copies
during submission:

```c
char statusbuf[4];
statusbuf[0] = (char)('0' + (status / 100) % 10);
statusbuf[1] = (char)('0' + (status / 10) % 10);
statusbuf[2] = (char)('0' + status % 10);
statusbuf[3] = '\0';
```

### Step 4.2 — Queue payloads

A response is a queue of owned payloads. One response submission uses one data
provider that drains the queue:

```c
typedef struct h2_out {
  uint8_t *payload;
  size_t len;
  size_t off;
  int eof;
  struct h2_out *next;
} h2_out_t;

typedef struct h2_stream {
  /* ... */
  h2_out_t *out_head;
  h2_out_t *out_tail;
  int resp_opened;
  int resp_closed;
} h2_stream_t;
```

`respond_msg()` copies a borrowed body into a node. The zero-copy variant,
added later, takes ownership of an already-owned buffer instead.

`respond_begin()` submits the response with a provider:

```c
nghttp2_data_provider provider;
provider.source.ptr = (void *)(intptr_t)sid;
provider.read_callback = on_data_read;

nghttp2_submit_response(s->ng, sid, nva, nvlen, &provider);
```

### Step 4.3 — Let nghttp2 pull the queue

The provider reports the head payload to nghttp2. It must not copy the payload:

```c
static ssize_t on_data_read(nghttp2_session *ng, int32_t sid, uint8_t *buf,
                            size_t length, uint32_t *flags,
                            nghttp2_data_source *source, void *user_data) {
  (void)ng;
  (void)buf;

  http2_session_t *s = user_data;
  int32_t id = (int32_t)(intptr_t)source->ptr;
  h2_stream_t *st = find_stream(s, id);

  if (st == NULL || st->out_head == NULL) {
    return NGHTTP2_ERR_DEFERRED;
  }

  size_t rem = st->out_head->len - st->out_head->off;
  size_t n = rem < length ? rem : length;

  *flags |= NGHTTP2_DATA_FLAG_NO_COPY;

  if (st->out_head->next == NULL && n == rem &&
      (st->out_head->eof || st->trailers == 1)) {
    *flags |= NGHTTP2_DATA_FLAG_EOF;
    if (st->trailers == 1) {
      *flags |= NGHTTP2_DATA_FLAG_NO_END_STREAM;
    }
  }

  return (ssize_t)n;
}
```

`NO_COPY` means nghttp2 will frame the bytes but expects a `send_data_callback`
to actually put them on the wire. Without that callback, the payload is reported
but never sent.

An empty queue returns `NGHTTP2_ERR_DEFERRED`. When a handler appends later, the
sender must call `nghttp2_session_resume_data()` so nghttp2 asks again:

```c
if (st->data_deferred) {
  st->data_deferred = 0;
  nghttp2_session_resume_data(s->ng, sid);
}
```

### Step 4.4 — Flush at the right time

`http2_session_feed()` already flushes when nghttp2 has output. Handlers that
respond outside a feed callback call `http2_flush()`:

```c
int http2_flush(http2_session_t *s) {
  if (!nghttp2_session_want_write(s->ng)) {
    return 0;
  }

  int srv = nghttp2_session_send(s->ng);
  if (srv == NGHTTP2_ERR_WOULDBLOCK) {
    return 0; /* output remains queued; retry when writable */
  }
  return srv == 0 ? 0 : -1;
}
```

`NGHTTP2_ERR_WOULDBLOCK` is not a failure. The send callback parked its offsets,
nghttp2 still wants to write, and the layer must wait for writability rather than
tearing the connection down. Both `http2_flush()` and the trailing send inside
`http2_session_feed()` must treat it as “try again later.”

Do not call `session_send()` from inside a data provider or send callback.
nghttp2 is already inside a send operation. The feed trailing send or an
explicit flush is the right place.

### Phase 4 pain log

1. **The send callback still assumes every write succeeds.** A partial write
   would lose the rest of a control frame.
2. **`NO_COPY` has no `send_data` implementation yet.** The queue reports bytes
   but cannot deliver them.
3. **No payload size capping.** `read_length` must not report more than the
   connection or stream window allows.
4. **No streaming mode.** Every request is unary.
5. **No trailer ordering.** `trailers` is mentioned but not implemented.
6. **The response queue is still per-stream and unsynchronized.** That is correct
   for worker-pinned sessions, but it must be enforced.

Phase 5 adds the write path and zero-copy ownership.

## Phase 5 — Partial writes and owned payloads

### Step 5.1 — Park one partial frame

`session_send()` stops at the first `WOULDBLOCK`. At most one frame can be
mid-flight per session. Track its header and payload offsets:

```c
typedef struct h2_parked {
  int active;
  int32_t sid;
  h2_out_t *msg;
  size_t flen;
  size_t hdr_sent;
  size_t sent;
} h2_parked_t;

struct http2_session {
  /* ... */
  h2_parked_t parked;
};
```

The node stays queued until it is fully framed. The parked state only remembers
how much of this frame has been written.

### Step 5.2 — Write the header and payload together

`send_data_callback` receives the 9-byte frame header separately from the
payload. Use `writev()` so the header and payload leave in one syscall when the
socket accepts both:

```c
static int on_send_data(nghttp2_session *ng, nghttp2_frame *frame,
                        const uint8_t *framehd, size_t length,
                        nghttp2_data_source *source, void *user_data) {
  (void)ng;
  (void)source;

  http2_session_t *s = user_data;
  h2_stream_t *st = find_stream(s, frame->hd.stream_id);
  h2_out_t *m = st != NULL ? st->out_head : NULL;
  h2_parked_t *p = &s->parked;

  if (m == NULL) {
    return NGHTTP2_ERR_CALLBACK_FAILURE;
  }

  if (!(p->active && p->msg == m)) {
    p->active = 1;
    p->sid = frame->hd.stream_id;
    p->msg = m;
    p->flen = length;
    p->hdr_sent = 0;
    p->sent = 0;
  }

  for (;;) {
    struct iovec iov[2];
    int n = 0;

    if (p->hdr_sent < 9) {
      iov[n].iov_base = (void *)(framehd + p->hdr_sent);
      iov[n].iov_len = 9 - p->hdr_sent;
      n++;
    }

    size_t prem = p->flen - p->sent;
    if (prem > 0) {
      iov[n].iov_base = m->payload + m->off + p->sent;
      iov[n].iov_len = prem;
      n++;
    }

    if (n == 0) {
      break;
    }

    ssize_t w = writev(s->fd, iov, n);
    if (w < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      return NGHTTP2_ERR_WOULDBLOCK;
    }
    if (w <= 0) {
      return NGHTTP2_ERR_CALLBACK_FAILURE;
    }

    size_t rem = (size_t)w;
    size_t hrem = 9 - p->hdr_sent;

    if (rem < hrem) {
      p->hdr_sent += rem;
      continue;
    }

    rem -= hrem;
    p->hdr_sent = 9;
    p->sent += rem;

    if (p->sent == p->flen) {
      m->off += p->flen;
      if (m->off == m->len) {
        int was_eof = m->eof;
        st->out_head = m->next;
        if (st->out_head == NULL) {
          st->out_tail = NULL;
        }
        if (was_eof) {
          st->resp_closed = 1;
        }
        free(m->payload);
        free(m);
      }
      p->active = 0;
      p->msg = NULL;
      return 0;
    }
  }

  return 0;
}
```

Two details matter:

- A zero-length payload still has a frame header; the loop must account for it.
- Padded frames are not supported here. Reject `frame->data.padlen != 0` rather
  than silently writing the wrong length.

### Step 5.3 — Cap the reported frame length

`on_data_read()` must not report more bytes than the peer's windows allow.
Otherwise nghttp2 may fail once cumulative sends pass a window boundary. Add a
read-length callback:

```c
static ssize_t on_read_length(nghttp2_session *ng, uint8_t frame_type,
                              int32_t sid, int32_t session_window,
                              int32_t stream_window,
                              uint32_t max_frame, void *user_data) {
  if (frame_type != NGHTTP2_DATA) {
    return (ssize_t)max_frame;
  }

  size_t cap = max_frame;
  h2_stream_t *st = find_stream(user_data, sid);

  if (st != NULL && st->out_head != NULL) {
    size_t rem = st->out_head->len - st->out_head->off;
    if (rem < cap) {
      cap = rem;
    }
  }
  if (session_window > 0 && (size_t)session_window < cap) {
    cap = (size_t)session_window;
  }
  if (stream_window > 0 && (size_t)stream_window < cap) {
    cap = (size_t)stream_window;
  }

  return (ssize_t)(cap > 0 ? cap : 1);
}
```

The floor of one keeps nghttp2 from treating a window of zero as a fatal
read-length error. nghttp2 only asks when it intends to send.

### Step 5.4 — Owned payloads

`http2_respond_msg()` copies because the caller's body may be borrowed.
`http2_respond_msg_owned()` takes a malloc-compatible pointer and transfers
ownership:

```c
int http2_respond_msg_owned(http2_session_t *s, int32_t sid, uint8_t *body,
                            size_t len, int end_stream);
```

The session frees the payload exactly once: after the final byte is framed, or
when the stream resets/closes first. Passing a stack pointer or a borrowed
pointer is a heap corruption bug. A NULL pointer with nonzero length is
rejected and freed.

### Phase 5 pain log

1. **Only one frame may be in flight.** `session_send()` stops at the first
   `WOULDBLOCK`; this is a real constraint of the write path.
2. **Padding is unsupported.** The code must reject padded DATA frames.
3. **No streaming requests yet.** Unary mode still buffers everything.
4. **No request trailers.** Response trailers are still pending.
5. **No explicit flow-control policy on receive.** The layer buffers first.
6. **No per-route state.** Handlers still cannot keep a parser or producer.

Phase 6 changes the receive side from “whole body” to “messages.”

## Phase 6 — Streaming requests and backpressure

### Step 6.1 — A message view, not a body

A streaming route needs message boundaries. The application supplies a pure
framing function:

```c
typedef size_t (*http2_msg_framing_cb)(const void *cfg,
                                       const http2_view_t *v,
                                       size_t cur, size_t avail);
```

The view has two segments: `a` is the residual from earlier chunks, `b` is the
newly arrived chunk. A message may lie entirely in one segment, or straddle the
seam. The framing function reads bytes through `http2_view_byte()` and never
assumes the message is contiguous:

```c
static inline uint8_t http2_view_byte(const http2_view_t *v, size_t i) {
  return i < v->an ? v->a[i] : v->b[i - v->an];
}
```

A framing function returns the total length of the message at `cur`, or `0`
when more bytes are needed. It must be pure and must not read past `avail`.

### Step 6.2 — Prefix, residual, messages

A streaming route may have a one-time params region before the first message.
Accumulate exactly `prefix_len` bytes into a module-owned `params` buffer. The
body buffer then holds only the residual of an incomplete message.

For every complete message:

- if it lies wholly in the residual, point into `st->body`;
- if it lies wholly in the new chunk, point into the chunk (zero copies);
- if it straddles the seam, compact it once.

Collect messages into a batch and hand the batch to the handler. Bound the batch
at `HTTP2_BATCH_MAX` messages so one huge chunk cannot monopolize the worker.

An incomplete tail stays in `st->body`. A streaming route must set
`http2_route_t.max_message_size`; the module allocates one fixed residual buffer
of that size when the route is bound to the stream. The buffer never grows or
doubles. A complete message longer than the configured maximum, or a leftover
tail that would fill the buffer, resets the stream.

The residual is stored as a ring: `body_head` is the first live byte and
`body_len` is the live length. Consuming messages advances the head; only the
new-chunk tail is appended, so the old residual is not memmoved on every chunk.
If the live region wraps, it is rotated back to offset zero when the framing
view is built. Appending is a hard-cap operation; overflow is a protocol error:

```c
static void reverse_bytes(uint8_t *p, size_t n) {
  for (size_t i = 0, j = n; i + 1 < j; i++, j--) {
    uint8_t t = p[i];
    p[i] = p[j - 1];
    p[j - 1] = t;
  }
}

static void ring_linearize(http2_stream_t *st) {
  if (st->body_len == 0) {
    st->body_head = 0;
    return;
  }
  if (st->body_head <= st->body_cap - st->body_len) {
    return;
  }
  size_t h = st->body_head;
  reverse_bytes(st->body, h);
  reverse_bytes(st->body + h, st->body_cap - h);
  reverse_bytes(st->body, st->body_cap);
  st->body_head = 0;
}

static int ring_append(http2_stream_t *st, const uint8_t *src, size_t n) {
  if (n == 0) {
    return 0;
  }
  if (st->body_cap == 0 || n > st->body_cap - st->body_len) {
    return -1;
  }
  size_t tail = (st->body_head + st->body_len) % st->body_cap;
  size_t first = st->body_cap - tail;
  if (first > n) {
    first = n;
  }
  memcpy(st->body + tail, src, first);
  if (first < n) {
    memcpy(st->body, src + first, n - first);
  }
  st->body_len += n;
  return 0;
}

static int ring_store_tail(http2_stream_t *st, const http2_view_t *v,
                           size_t cur, size_t left) {
  size_t old_rem = cur < v->an ? v->an - cur : 0;
  size_t new_len = left - old_rem;
  const uint8_t *new_tail = NULL;
  if (new_len > 0) {
    size_t off = cur > v->an ? cur - v->an : 0;
    new_tail = v->b + off;
  }
  if (old_rem > 0) {
    st->body_head += cur;
  } else {
    st->body_head = 0;
  }
  st->body_len = old_rem;
  if (st->body_len == 0) {
    st->body_head = 0;
  }
  return ring_append(st, new_tail, new_len);
}
```

### Step 6.3 — Attached and detached batches

A batch is either borrowed for the handler call or owned by the handler:

```c
typedef struct http2_msg {
  const uint8_t *msg;
  size_t len;
} http2_msg_t;

typedef struct http2_batch http2_batch_t;
```

An attached batch is a stack-style view over borrowed message pointers. It is
valid only for the handler call. A detached batch is one allocation containing
the batch header, the message array, and the message bytes. The handler may keep
it and release it later.

The same handler type serves both:

```c
typedef void (*http2_stream_fn)(http2_session_t *s, http2_stream_t *st,
                                http2_batch_t *batch);
```

The handler reads messages with `http2_batch_msgs()`, finishes with
`http2_batch_consume()` when it owns the batch, and calls
`http2_batch_release()` exactly once.

### Step 6.4 — Manual flow control

By default nghttp2 automatically credits received DATA. That is wrong for a
layer that may hold a detached batch after the handler returns. Turn automatic
window updates off:

```c
nghttp2_option_set_no_auto_window_update(opt, 1);
```

Now the module must credit bytes exactly once:

- unary bytes are credited when buffered;
- attached streaming messages are credited when the handler returns;
- detached streaming bytes stay uncredited until the application calls
  `http2_batch_consume()` or `http2_stream_consume()`.

While a detached batch is uncredited, the peer eventually stops sending DATA.
That is the intended backpressure. It is not a stall; it is the window doing its
job.

Credit the connection window even when the stream is already gone. A reset
stream must not leak connection credit and stall every other stream.

### Phase 6 pain log

1. **Straddling messages still need one copy.** That is the price of a two-segment
   view.
2. **Residual is capped, not eliminated.** A peer that never completes a message
   is reset at the cap.
3. **Detached batches require the application to consume and release.** Leaking
   one leaks flow-control credit, not just memory.
4. **Streaming and unary share a handler union.** The route decides the mode at
   HEADERS.
5. **No per-route state yet.** The handler still has to recover its parser from a
   global or from `args`.
6. **No response trailers or reset policy beyond PROTOCOL_ERROR.**

Phase 7 fills in the edges that make the library usable in a server.

## Phase 7 — State, trailers, drain, and ownership

### Step 7.1 — Per-stream application state

A route can ask the module to allocate zeroed per-stream state:

```c
typedef struct http2_route {
  /* ... */
  size_t state_size;
  void (*state_init)(void *ctx);
  void (*state_free)(void *ctx);
} http2_route_t;
```

At request HEADERS, after route resolution, allocate `state_size` bytes and run
`state_init()`. At stream close, run `state_free()` and then return the block to
the allocator/pool. The handler recovers it with `http2_stream_ctx(st)`.

Give each route its own struct type if it wants. A common pattern is a shared
`base` prefix so generic close/cancel code can inspect the route and stream id
without knowing the concrete type.

### Step 7.2 — Response trailers

Trailers are submitted after the final DATA frame, and they carry END_STREAM
themselves. The caller's `nghttp2_nv` array may be gone by then, so deep-copy it
when `http2_respond_trailers()` is called.

When the data provider reports the final payload, set
`NGHTTP2_DATA_FLAG_NO_END_STREAM` and submit the trailer block. The trailer
HEADERS must be ordered after the DATA, not instead of it.

### Step 7.3 — Reset and close ordering

A stream can disappear for several reasons:

- normal END_STREAM;
- application reset;
- protocol error;
- session teardown.

Free order matters. Release the parked frame reference first, then headers,
params, body, trailers, queued payloads, and application state. Do not free a
payload that is still referenced by `s->parked`; clear the parked slot and let
the queue destructor free the node once.

### Step 7.4 — Graceful drain and keepalive

A server cannot close a connection with active streams and call it graceful.
Expose two drain phases:

```c
int http2_session_shutdown_notice(http2_session_t *s);
int http2_session_goaway(http2_session_t *s, uint32_t error_code);
```

The notice submits a GOAWAY with a maximal last-stream id so existing streams
keep running. After roughly one RTT, the final GOAWAY pins the highest processed
stream id. Streams above it are refused; streams at or below it finish.

Expose the active stream count so the server can decide when the connection is
idle:

```c
size_t http2_session_active_streams(const http2_session_t *s);
```

Keepalive is a separate policy. The module only submits a PING and remembers the
ACK token:

```c
int http2_session_ping(http2_session_t *s, uint64_t opaque);
int http2_session_ping_ack(http2_session_t *s, uint64_t *opaque);
```

The caller owns retry and timeout decisions. Idle detection is different from
keepalive: idle should use application activity, not PING traffic. Expose a
monotonic count of inbound HEADERS and DATA so the server can distinguish the
two:

```c
uint64_t http2_session_app_activity(const http2_session_t *s);
```

### Step 7.5 — One owner thread

nghttp2 sessions are not thread-safe. Every public session operation except
construction and the process-wide budget setter must run on the thread that
created the session. Record the owner at construction and enforce it:

```c
struct http2_session {
  // ...
  pthread_t owner;
};

static void h2_require_owner(const http2_session_t *s) {
  if (s != NULL && !pthread_equal(s->owner, pthread_self())) {
    abort();
  }
}
```

This is not a lock. It is a contract check. The async engine already pins a
connection to one worker; the HTTP/2 layer relies on that.

### Step 7.6 — Pools and cleanup

Allocating a stream, header, outbound node, and application state per request is
correct but expensive. Add a small thread-local size-class pool for the fixed
node types and the route state. Since the worker owns its sessions, the pool
needs no locks.

Provide a cleanup hook for worker shutdown:

```c
void http2_thread_cleanup(void);
```

It returns the freelists to the allocator. Sessions themselves must be freed
before this runs.

### Phase 7 pain log

1. **Route registration is still process-global.** That matches the current
   server, but it is not a per-listener configuration object.
2. **No TLS.** This layer is h2c prior-knowledge only.
3. **No HTTP/1.1 upgrade.** The server must know it is speaking HTTP/2.
4. **No server push or priority policy.** nghttp2 can do more; this layer does
   not expose it.
5. **Padding is rejected, not supported.**
6. **The public header still exposes internal fields.** Handlers need some of
   them, but the rest are implementation details.

Phase 8 closes the library and shows how to use it.

## Phase 8 — Close the library

### Step 8.1 — The final public surface

The final header is `include/http2_server.h`. Its public groups are:

```c
/* session */
typedef struct http2_session http2_session_t;
typedef struct http2_transport http2_transport_t;
typedef enum {
  HTTP2_FLOW_INHERIT = 0,
  HTTP2_FLOW_DEFAULT,
  HTTP2_FLOW_LOW_LATENCY,
  HTTP2_FLOW_BULK,
} http2_flow_profile_t;
http2_session_t *http2_session_new(int fd, uint32_t stream_window_size,
                                   uint32_t connection_window_size);
http2_session_t *http2_session_new_transport(const http2_transport_t *t, int fd,
                                             uint32_t stream_window_size,
                                             uint32_t connection_window_size);
int http2_session_set_flow_profile(http2_session_t *s,
                                   http2_flow_profile_t profile);
http2_flow_profile_t http2_session_flow_profile(const http2_session_t *s);
int http2_session_feed(http2_session_t *s, const uint8_t *data, size_t len);
int http2_flush(http2_session_t *s);
void http2_session_free(http2_session_t *s);
void http2_thread_cleanup(void);

/* routing */
int http2_route_add(const http2_route_t *r);

/* request inspection */
int32_t http2_stream_id(const http2_stream_t *st);
void *http2_stream_ctx(const http2_stream_t *st);
const http2_header_t *http2_stream_headers(const http2_stream_t *st);
const uint8_t *http2_header_get(const http2_stream_t *st, const char *name,
                                size_t namelen, size_t *vlen);
const uint8_t *http2_prefix(const http2_stream_t *st, size_t *len);

/* responses */
int http2_respond_begin(http2_session_t *s, int32_t sid, int status,
                        const nghttp2_nv *headers, size_t nheaders);
int http2_respond_msg(http2_session_t *s, int32_t sid,
                      const uint8_t *body, size_t len, int end_stream);
int http2_respond_msg_owned(http2_session_t *s, int32_t sid, uint8_t *body,
                            size_t len, int end_stream);
int http2_respond_end(http2_session_t *s, int32_t sid);
int http2_respond_trailers(http2_session_t *s, int32_t sid,
                           const nghttp2_nv *nva, size_t nvlen);
void http2_rst_stream(http2_session_t *s, int32_t sid);

/* streaming batches */
const http2_msg_t *http2_batch_msgs(const http2_batch_t *b, size_t *n);
int http2_batch_owned(const http2_batch_t *b);
size_t http2_batch_consume_bytes(const http2_batch_t *b);
int http2_batch_consume(http2_session_t *s, http2_batch_t *b);
void http2_batch_release(http2_batch_t *b);
int http2_stream_consume(http2_session_t *s, int32_t sid, size_t n);

/* limits and lifecycle */
void http2_set_global_unary_budget(size_t bytes);
int http2_session_set_unary_budget(http2_session_t *s, size_t bytes);
size_t http2_session_unary_bytes(const http2_session_t *s);
void http2_set_global_queue_limit(size_t bytes);
void http2_set_tenant_queue_limit(size_t bytes);
int http2_session_set_queue_limits(http2_session_t *s, size_t stream_limit,
                                   size_t connection_limit);
size_t http2_session_queued_bytes(const http2_session_t *s);
int http2_session_set_tenant_id(http2_session_t *s, uint64_t tenant_id);
uint64_t http2_session_app_activity(const http2_session_t *s);
size_t http2_session_active_streams(const http2_session_t *s);
int http2_session_shutdown_notice(http2_session_t *s);
int http2_session_goaway(http2_session_t *s, uint32_t error_code);
int http2_session_ping(http2_session_t *s, uint64_t opaque);
int http2_session_ping_ack(http2_session_t *s, uint64_t *opaque);

/* escape hatches */
nghttp2_session *http2_raw(http2_session_t *s);
```

The full header defines `http2_route_t`, `http2_header_t`, `http2_view_t`,
`http2_msg_t`, `http2_transport_t`, and the two handler typedefs.
`http2_stream_t` is opaque: use `http2_stream_id()`, `http2_stream_ctx()`, and
`http2_stream_headers()` to inspect it. Read the comments there for the
per-field ownership rules.

`http2_session_new()` keeps the original direct-fd write path and takes the
receive-window targets explicitly; pass `HTTP2_DEFAULT_WINDOW_SIZE` for the
standard 65535-byte window. Zero is rejected.
`http2_session_new_transport()` uses the same response API but sends framed
bytes through caller-supplied `write`/`writev` callbacks, so a TLS module can
encrypt without changing `http2_respond_msg()` or duplicating responses.

Flow control is selected by profile, not by exposing the internal target/source/
trigger/amount policy. `http2_session_set_flow_profile()` applies to streams
created after the call; existing streams keep their profile. A route may set
`http2_route_t.flow_profile` to override the session profile, with
`HTTP2_FLOW_INHERIT` meaning "use the session profile".

Streaming routes must also set `http2_route_t.max_message_size`. The module
allocates one fixed residual buffer of that size per streaming stream; it does
not grow by doubling. A complete message larger than that maximum, or an
incomplete tail that would fill the buffer, resets the stream.

### Flow-control adapter

The public API exposes profiles only. The policy target/source/trigger/amount
choices, per-window accounting, and validation stay private. The adapter maps a
profile to a policy, initializes one flow window per stream and one for the
connection, turns application/consumption events into state changes, and maps
the resulting credit actions to nghttp2.

```c
static uint64_t h2_now_ms(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000u + (uint64_t)(ts.tv_nsec / 1000000L);
}

static int h2_flow_profile_internal(http2_flow_profile_t profile) {
  switch (profile) {
  case HTTP2_FLOW_DEFAULT:
    return FLOW_PROFILE_DEFAULT;
  case HTTP2_FLOW_LOW_LATENCY:
    return FLOW_PROFILE_LOW_LATENCY;
  case HTTP2_FLOW_BULK:
    return FLOW_PROFILE_BULK;
  default:
    return -1;
  }
}

static int h2_stream_flow_init(http2_session_t *s, http2_stream_t *st,
                               uint32_t target,
                               http2_flow_profile_t public_profile) {
  int profile = h2_flow_profile_internal(public_profile);
  if (profile < 0 || target == 0) {
    return -1;
  }
  h2_flow_t *flow = h2_small_alloc(sizeof *flow);
  if (flow == NULL) {
    return -1;
  }
  flow_policy_profile((flow_profile_t)profile, target, s->connection_window,
                      &flow->policy);
  uint32_t warnings = 0;
  flow_error_t err = flow_policy_validate(&flow->policy, &warnings);
  if (err != FLOW_OK || flow_window_init(&flow->window, &flow->policy, target) !=
                           0) {
    log_error("[h2] stream %d: invalid flow policy: %s", (int)st->id,
              flow_error_string(err));
    h2_small_free(flow, sizeof *flow);
    return -1;
  }
  st->flow = flow;
  return 0;
}

static void h2_flow_received(http2_session_t *s, http2_stream_t *st, size_t n) {
  if (!s->flow_enabled || n == 0) {
    return;
  }
  flow_window_on_received(&s->conn_flow, (uint32_t)n);
  if (st != NULL && st->flow != NULL) {
    flow_window_on_received(&st->flow->window, (uint32_t)n);
  }
}

static int h2_apply_stream_credit(http2_session_t *s, http2_stream_t *st,
                                  flow_credit_t credit) {
  if (credit.mode == FLOW_CREDIT_EXACT) {
    if (nghttp2_session_consume_stream(s->session, st->id, credit.bytes) != 0) {
      log_error("[h2] stream %d: consume_stream failed", (int)st->id);
      return -1;
    }
    return 0;
  }
  if (credit.mode == FLOW_CREDIT_TOP_UP) {
    if (nghttp2_session_set_local_window_size(s->session, NGHTTP2_FLAG_NONE,
                                              st->id,
                                              (int32_t)credit.window) != 0) {
      log_error("[h2] stream %d: window top-up failed", (int)st->id);
      return -1;
    }
  }
  return 0;
}

static int h2_apply_connection_credit(http2_session_t *s, flow_credit_t credit) {
  if (credit.mode == FLOW_CREDIT_EXACT) {
    if (nghttp2_session_consume_connection(s->session, credit.bytes) != 0) {
      log_error("[h2] connection consume failed");
      return -1;
    }
    return 0;
  }
  if (credit.mode == FLOW_CREDIT_TOP_UP) {
    if (nghttp2_session_set_local_window_size(s->session, NGHTTP2_FLAG_NONE, 0,
                                              (int32_t)credit.window) != 0) {
      log_error("[h2] connection window top-up failed");
      return -1;
    }
  }
  return 0;
}

int http2_session_set_flow_profile(http2_session_t *s,
                                   http2_flow_profile_t profile) {
  if (s == NULL) {
    return -1;
  }
  h2_require_owner(s);
  int internal = h2_flow_profile_internal(profile);
  if (internal < 0) {
    return -1;
  }
  flow_policy_t stream_policy;
  flow_policy_t conn_policy;
  flow_policy_profile((flow_profile_t)internal, s->stream_window,
                      s->connection_window, &stream_policy);
  flow_policy_profile((flow_profile_t)internal, s->connection_window,
                      s->connection_window, &conn_policy);
  uint32_t warnings = 0;
  if (flow_policy_validate(&stream_policy, &warnings) != FLOW_OK ||
      flow_policy_validate(&conn_policy, &warnings) != FLOW_OK) {
    return -1;
  }
  s->flow_profile = profile;
  s->flow_policy = stream_policy;
  s->conn_flow_policy = conn_policy;
  s->flow_enabled = 1;
  return 0;
}

http2_flow_profile_t http2_session_flow_profile(const http2_session_t *s) {
  return s != NULL ? s->flow_profile : HTTP2_FLOW_INHERIT;
}
```

`h2_consume_stream()` feeds these windows from the existing buffering paths:
unary and residual bytes are reported when the module accepts them, attached
streaming batches after the handler returns, and detached batches when the app
calls `http2_batch_consume()`. The adapter applies stream and connection credits
separately.

### Static route compression

Compression is selected statically per route and applies to both request and
response streams. It does not use HTTP `Content-Encoding` or `Accept-Encoding`;
both peers must share the route contract.

```c
typedef enum {
  HTTP2_COMPRESSION_OFF = 0,
  HTTP2_COMPRESSION_GZIP,
  HTTP2_COMPRESSION_ZSTD,
  HTTP2_COMPRESSION_BROTLI,
} http2_compression_t;

typedef enum {
  HTTP2_COMPRESSION_FAST = 0,
  HTTP2_COMPRESSION_BALANCED,
  HTTP2_COMPRESSION_BEST,
} http2_compression_profile_t;
```

A compressed route decompresses request DATA before prefix extraction and
`msg_framing`, then compresses the entire response byte stream. Unary and
streaming use the same policy; there is no per-message flag or size threshold.
The entire request stream is capped at `HTTP2_MAX_DECOMPRESSED`, while unary
requests also remain bounded by `HTTP2_MAX_BODY`.

### Outbound response-queue limits

Outbound payload bytes are capped at four levels: stream, connection, tenant,
and process. Limits are checked before a response node is queued; exceeding any
level fails the submission and the caller resets the stream. Structural control
frames such as RST_STREAM and GOAWAY are not part of the payload queue. The
tenant key is typically a hash of the peer IP, so all connections from the same
IP share one quota.

```c
static size_t h2_queue_limit(size_t value, size_t fallback) {
  return value != 0 ? value : fallback;
}

static uint32_t h2_tenant_bucket(uint64_t id) {
  id ^= id >> 33;
  id *= 0xff51afd7ed558ccdULL;
  id ^= id >> 33;
  return (uint32_t)(id & (H2_TENANT_BUCKETS - 1));
}

static int h2_queue_charge(uint64_t tenant, size_t n) {
  if (n == 0) {
    return 0;
  }
  pthread_mutex_lock(&g_queue_mu);
  if (g_global_queued > g_global_queue_limit ||
      n > g_global_queue_limit - g_global_queued) {
    pthread_mutex_unlock(&g_queue_mu);
    return -1;
  }

  h2_tenant_entry_t *entry = NULL;
  uint32_t bucket = 0;
  int created = 0;
  if (tenant != 0) {
    bucket = h2_tenant_bucket(tenant);
    for (entry = g_tenants[bucket]; entry != NULL; entry = entry->next) {
      if (entry->id == tenant) {
        break;
      }
    }
    if (entry == NULL) {
      entry = calloc(1, sizeof *entry);
      if (entry == NULL) {
        pthread_mutex_unlock(&g_queue_mu);
        return -1;
      }
      entry->id = tenant;
      entry->next = g_tenants[bucket];
      g_tenants[bucket] = entry;
      created = 1;
    }
    if (entry->bytes > g_tenant_queue_limit ||
        n > g_tenant_queue_limit - entry->bytes) {
      if (created) {
        g_tenants[bucket] = entry->next;
        free(entry);
      }
      pthread_mutex_unlock(&g_queue_mu);
      return -1;
    }
  }

  g_global_queued += n;
  if (entry != NULL) {
    entry->bytes += n;
  }
  pthread_mutex_unlock(&g_queue_mu);
  return 0;
}

static void h2_queue_release(uint64_t tenant, size_t n) {
  if (n == 0) {
    return;
  }
  pthread_mutex_lock(&g_queue_mu);
  g_global_queued = n <= g_global_queued ? g_global_queued - n : 0;
  if (tenant != 0) {
    uint32_t bucket = h2_tenant_bucket(tenant);
    h2_tenant_entry_t **link = &g_tenants[bucket];
    while (*link != NULL) {
      h2_tenant_entry_t *entry = *link;
      if (entry->id == tenant) {
        entry->bytes = n <= entry->bytes ? entry->bytes - n : 0;
        if (entry->bytes == 0) {
          *link = entry->next;
          free(entry);
        }
        break;
      }
      link = &entry->next;
    }
  }
  pthread_mutex_unlock(&g_queue_mu);
}

static int h2_queue_fits(const http2_session_t *s, const http2_stream_t *st,
                         size_t n) {
  if (n > s->stream_queue_limit ||
      st->queued_bytes > s->stream_queue_limit - n) {
    return 0;
  }
  if (n > s->connection_queue_limit ||
      s->queued_bytes > s->connection_queue_limit - n) {
    return 0;
  }
  return 1;
}

void http2_set_global_queue_limit(size_t bytes) {
  pthread_mutex_lock(&g_queue_mu);
  g_global_queue_limit = h2_queue_limit(bytes, HTTP2_DEFAULT_GLOBAL_QUEUE);
  pthread_mutex_unlock(&g_queue_mu);
}

void http2_set_tenant_queue_limit(size_t bytes) {
  pthread_mutex_lock(&g_queue_mu);
  g_tenant_queue_limit = h2_queue_limit(bytes, HTTP2_DEFAULT_TENANT_QUEUE);
  pthread_mutex_unlock(&g_queue_mu);
}

int http2_session_set_queue_limits(http2_session_t *s, size_t stream_limit,
                                   size_t connection_limit) {
  if (s == NULL) {
    return -1;
  }
  h2_require_owner(s);
  stream_limit = h2_queue_limit(stream_limit, HTTP2_DEFAULT_STREAM_QUEUE);
  connection_limit =
      h2_queue_limit(connection_limit, HTTP2_DEFAULT_CONNECTION_QUEUE);
  if (connection_limit < stream_limit) {
    return -1;
  }
  s->stream_queue_limit = stream_limit;
  s->connection_queue_limit = connection_limit;
  return 0;
}

size_t http2_session_queued_bytes(const http2_session_t *s) {
  return s != NULL ? s->queued_bytes : 0;
}

int http2_session_set_tenant_id(http2_session_t *s, uint64_t tenant_id) {
  if (s == NULL) {
    return -1;
  }
  h2_require_owner(s);
  if (s->queued_bytes != 0) {
    return -1;
  }
  s->tenant_id = tenant_id;
  return 0;
}
```

`http2_session_queued_bytes()` is useful for application-side backpressure:
stop producing when the connection is near its limit instead of waiting for the
next enqueue to fail.

### Step 8.2 — Register routes

A route is configuration, not per-request state. Register once before serving:

```c
static http2_route_t routes[] = {
    {
        .path = "/3/7",
        .mode = HTTP2_UNARY,
        .msg_framing = msg_framing,
        .framing_cfg = &schema_3_7,
        .state_size = sizeof(stream_state_t),
        .handler.unary = handle_getstats,
    },
};

for (size_t i = 0; i < sizeof(routes) / sizeof(routes[0]); i++) {
  if (http2_route_add(&routes[i]) != 0) {
    return EXIT_FAILURE;
  }
}
```

`r->path` and `r->framing_cfg` must outlive the process. The route table copies
the route struct, not the pointed-to strings or config.

A streaming route must supply `msg_framing`. Without a framing rule there are no
message boundaries, so the request is reset.

### Step 8.3 — Handle a unary request

```c
static void handle_getstats(http2_session_t *s, http2_stream_t *st,
                            const uint8_t *body, size_t len) {
  /* The route's framing rule guaranteed one complete message. */
  uint32_t status = rd32(body + 0);
  uint16_t active = rd16(body + 4);
  uint16_t idle = rd16(body + 6);
  uint8_t flags = body[8];
  uint16_t nlen = rd16(body + 9);
  const uint8_t *name = body + 11;

  (void)status;
  (void)active;
  (void)idle;
  (void)flags;

  http2_respond_msg(s, http2_stream_id(st), name, nlen, 1);
}
```

The handler runs synchronously on the worker thread. `respond_msg()` copies the
borrowed name into the outbox; the handler may return immediately. The trailing
send in `http2_session_feed()` flushes it.

### Step 8.4 — Handle a streaming request

```c
static void handle_stream(http2_session_t *s, http2_stream_t *st,
                          http2_batch_t *batch) {
  size_t n = 0;
  const http2_msg_t *msgs = http2_batch_msgs(batch, &n);

  for (size_t i = 0; i < n; i++) {
    /* msgs[i].msg is borrowed for this call (attached batch). */
    process_message(msgs[i].msg, msgs[i].len);
  }

  if (http2_batch_owned(batch)) {
    http2_batch_consume(s, batch);
    http2_batch_release(batch);
  }
}
```

For an attached batch, release is a no-op and the module already credits flow
control when the handler returns. For a detached batch, the handler owns the
bytes: consume the credit, then release.

A streaming handler can respond with many `http2_respond_msg()` calls and finish
with `http2_respond_end()` or `http2_respond_trailers()`.

### Step 8.5 — Drive it from the async engine

The HTTP/2 layer does not own a socket or a thread. The async engine owns both.
This layer uses nghttp2's send callback directly, so it does **not** call
`task_write()` and it does **not** set `write_buffer`. It reports whether it
still needs writability through the task state:

```c
static void session_process(io_task_t *task) {
  client_ctx_t *ctx = task->context;

  if (ctx->h2 == NULL) {
    ctx->h2 = http2_session_new(task->fd, stream_window, connection_window);
    if (ctx->h2 == NULL) {
      task->state = TASK_COMPLETED;
      return;
    }
    http2_session_set_unary_budget(ctx->h2, session_budget);
  }

  if (task->bytes_buffered > 0) {
    if (http2_session_feed(ctx->h2, task->read_buffer,
                           task->bytes_buffered) != 0) {
      task->state = TASK_COMPLETED;
      return;
    }
    task->bytes_buffered = 0; /* nghttp2 copied/consumed the whole chunk */
  }

  if (http2_flush(ctx->h2) != 0) {
    task->state = TASK_COMPLETED;
    return;
  }

  int want_write =
      nghttp2_session_want_write(http2_raw(ctx->h2)) != 0;

  if (task->read_closed) {
    /* Half-close: flush queued frames, then finish. */
    task->state = want_write ? TASK_WAIT_WRITE : TASK_COMPLETED;
    return;
  }

  task->state = want_write ? TASK_WAIT_WRITE : TASK_ACTIVE;
}
```

`TASK_WAIT_WRITE` is the handler-owned output path. A writable event runs
`session_process()` again so nghttp2 can flush; it does not run the engine's
`write_step()`. Reads stay enabled while output is blocked, so a peer can keep
sending DATA and WINDOW_UPDATE frames.

When the peer closes its write side, `task->read_closed` becomes true. Do not
finish immediately: flush nghttp2's queued output first. If it is blocked, wait
for writability; when it no longer wants to write, complete the task and let the
callback free `task->read_buffer`.

Set the descriptor nonblocking before creating the session. The send callbacks
assume `EAGAIN` is possible. If the engine drains a socket and HTTP/2 still wants
to write, the task must enter `TASK_WAIT_WRITE` or the output will sit queued
until the next unrelated event.

### Step 8.6 — Drain and release

A graceful server does this:

1. stop accepting new connections;
2. call `http2_session_shutdown_notice()` on each session;
3. wait roughly one RTT;
4. call `http2_session_goaway()` to pin the last processed stream;
5. keep feeding/flushing until `http2_session_active_streams() == 0` and
   nghttp2 no longer wants to read or write;
6. call `http2_session_free()`;
7. call `http2_thread_cleanup()` when the worker exits.

`http2_session_free()` frees every stream, queued payload, parked frame, and
application context. It is not safe to call from a non-owner thread.

### Step 8.7 — Ownership rules to remember

- A `http2_session_t` is worker-pinned. Do not call its API from another thread.
- A `http2_stream_t`, its headers, and its unary body are borrowed. Copy
  anything you need after the handler returns.
- An attached `http2_batch_t` is borrowed for the handler call.
- A detached `http2_batch_t` is owned by the handler. Consume then release it.
- `http2_respond_msg()` copies the body.
- `http2_respond_msg_owned()` takes ownership and frees exactly once.
- `http2_route_add()` copies the route struct, not `path` or `framing_cfg`.
- `http2_header_get()` returns a borrowed value. Header fields are binary, not
  NUL-terminated.
- `http2_raw()` is an escape hatch. You are responsible for flushing and for not
  breaking nghttp2's state.

### Phase 8 checklist

- [ ] I can create a session on a nonblocking fd.
- [ ] I can feed bytes and flush output.
- [ ] I use `TASK_WAIT_WRITE` when nghttp2 still wants to write.
- [ ] I keep reading while output is blocked.
- [ ] On peer EOF I flush queued output before completing the task.
- [ ] My routes are registered before serving.
- [ ] My unary framing rule validates exactly one message.
- [ ] My streaming framing rule returns 0 until a whole message is available.
- [ ] I consume and release detached batches.
- [ ] I copy anything I keep past a handler call.
- [ ] I never call the session from another worker.
- [ ] I bound unary body memory with a session and process budget.
- [ ] I drain with GOAWAY before freeing the session.
- [ ] I call `http2_thread_cleanup()` when the worker exits.

## Implementation companion: complete source coverage

The phases explain the design in order. This companion fills in the source-level
helpers that are easy to miss when reading the architecture alone. Each section
contains the functions that belong to that subsystem; the code is not a
replacement for the earlier explanations, but it is the complete implementation
surface.

### Memory pools and thread cleanup

These helpers keep the hot path allocation-light. Small nodes are recycled through per-thread size-class free lists; larger nodes fall back to malloc. `http2_thread_cleanup()` releases the worker thread's free lists at shutdown.

```c
static int h2_class(size_t sz) {
  size_t c = (sz + (H2_CLASS_UNIT - 1)) / H2_CLASS_UNIT;
  return (c >= 1 && c <= H2_CLASS_MAX) ? (int)c : 0;
}

static void *h2_small_alloc(size_t sz) {
  int c = h2_class(sz);
  if (c == 0) {
    return calloc(1, sz);
  }
  size_t bsz = (size_t)c * H2_CLASS_UNIT;
  void *p = h2_pool[c - 1];
  if (p != NULL) {
    h2_pool[c - 1] = *(void **)p;
    h2_pool_n[c - 1]--;
  } else {
    p = calloc(1, bsz);
    if (p == NULL) {
      return NULL;
    }
  }
  return memset(p, 0, bsz);
}

static void h2_small_free(void *p, size_t sz) {
  if (p == NULL) {
    return;
  }
  int c = h2_class(sz);
  if (c == 0 || h2_pool_n[c - 1] >= H2_POOL_MAX) {
    free(p);
    return;
  }
  *(void **)p = h2_pool[c - 1];
  h2_pool[c - 1] = p;
  h2_pool_n[c - 1]++;
}

static void *h2_node_alloc(void) {
  return h2_small_alloc(H2_NODE_SZ);
}

static void h2_node_free(void *p) {
  h2_small_free(p, H2_NODE_SZ);
}

void http2_thread_cleanup(void) {
  for (int c = 0; c < H2_CLASS_MAX; c++) {
    void *p = h2_pool[c];
    while (p != NULL) {
      void *next = *(void **)p;
      free(p);
      p = next;
    }
    h2_pool[c] = NULL;
    h2_pool_n[c] = 0;
  }
}
```

### Unary budgets and batch ownership

Unary accounting is checked before bytes are retained, then charged to both the session and process budgets. Batches track whether their messages are borrowed from the residual feed or owned by the module, so release uses the right lifetime path.

```c
static int h2_charge_unary(http2_session_t *s, http2_stream_t *st, size_t n) {
  if (n == 0) {
    return 0;
  }
  if (s->unary_budget != 0 &&
      (s->unary_bytes > s->unary_budget ||
       n > s->unary_budget - s->unary_bytes)) {
    return -1;
  }
  size_t limit = atomic_load_explicit(&g_unary_budget, memory_order_relaxed);
  if (limit != 0) {
    size_t cur = atomic_load_explicit(&g_unary_bytes, memory_order_relaxed);
    for (;;) {
      if (cur > limit || n > limit - cur) {
        return -1;
      }
      if (atomic_compare_exchange_weak_explicit(
              &g_unary_bytes, &cur, cur + n, memory_order_relaxed,
              memory_order_relaxed)) {
        break;
      }
    }
  } else {
    atomic_fetch_add_explicit(&g_unary_bytes, n, memory_order_relaxed);
  }
  s->unary_bytes += n;
  st->unary_charged += n;
  return 0;
}

static void h2_uncharge_unary_n(http2_session_t *s, http2_stream_t *st,
                                size_t n) {
  if (n == 0) {
    return;
  }
  st->unary_charged = n <= st->unary_charged ? st->unary_charged - n : 0;
  s->unary_bytes = n <= s->unary_bytes ? s->unary_bytes - n : 0;
  atomic_fetch_sub_explicit(&g_unary_bytes, n, memory_order_relaxed);
}

static void h2_uncharge_unary(http2_session_t *s, http2_stream_t *st) {
  h2_uncharge_unary_n(s, st, st->unary_charged);
}

const http2_msg_t *http2_batch_msgs(const http2_batch_t *b, size_t *n) {
  if (n != NULL) {
    *n = b != NULL ? b->n : 0;
  }
  return b != NULL ? b->msgs : NULL;
}

int http2_batch_owned(const http2_batch_t *b) { return b != NULL ? b->owned : 0; }

void http2_batch_release(http2_batch_t *b) {
  if (b != NULL && b->owned) {
    free(b); /* single allocation: struct + msgs + bytes */
  }
}
```

### Routing, header cleanup, and output lists

Routing resolves the registered route and validates the wire request shape. The path is resolved while the request headers are being walked, so `on_frame_recv` uses the route cached on the stream instead of rescanning the header list. The cleanup helpers release header arrays and queued output nodes without freeing caller-owned payloads.

Short paths are optimized: up to eight NUL-free bytes are packed into a `uint64_t`, hashed with SplitMix64, and compared with integer equality. Longer paths, or paths containing NUL, fall back to the byte-keyed FNV hash. Registry entries store `path_len`, so neither path uses `strlen()` during lookup.

```c
static uint32_t route_hash(const uint8_t *p, size_t n) {
  uint32_t h = 2166136261u;
  for (size_t i = 0; i < n; i++) {
    h ^= p[i];
    h *= 16777619u;
  }
  return h;
}

static uint64_t route_pack(const uint8_t *p, size_t n) {
  uint64_t k = 0;
  for (size_t i = 0; i < n; i++) {
    k |= (uint64_t)p[i] << (8 * (7 - i));
  }
  return k;
}

static uint64_t route_mix(uint64_t x) {
  x += 0x9e3779b97f4a7c15ull;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ull;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebull;
  return x ^ (x >> 31);
}

static void wire_request_route(http2_session_t *s, http2_stream_t *st,
                               const http2_route_t *r) {
  if (r == NULL) {
    log_error("[h2] stream %d: unknown route", (int)st->id);
    http2_rst_stream(s, st->id);
    return;
  }
  if (r->mode == HTTP2_STREAMING) {
    /* Without a framing rule there are no message boundaries; the handler
     * cannot be called meaningfully, so reject the request. */
    if (r->msg_framing == NULL) {
      log_error("[h2] stream %d: streaming route has no framing", (int)st->id);
      http2_rst_stream(s, st->id);
      return;
    }
    http2_stream_set_stream(st, r->params_len, r->msg_framing, r->framing_cfg,
                            r->handler.stream);
    http2_stream_set_detach(st, r->detach);
  } else {
    http2_stream_set_unary(st, r->msg_framing, r->framing_cfg,
                           r->handler.unary);
  }
  st->route = r;
  /* Per-route stream state: allocate zeroed ctx of the route's size and run its
   * constructor, so handlers can cast st->ctx to their route-specific struct. */
  if (r->state_size > 0 && st->ctx == NULL) {
    st->ctx = h2_small_alloc(r->state_size);
    if (st->ctx == NULL) {
      log_error("[h2] stream %d: out of memory for state", (int)st->id);
      http2_rst_stream(s, st->id);
      return;
    }
    if (r->state_init != NULL) {
      r->state_init(st->ctx);
    }
  }
}

static void free_headers(http2_header_t *h) {
  while (h != NULL) {
    http2_header_t *next = h->next;
    /* name/value share one allocation (value aliases into it): free once. */
    free(h->name);
    h2_node_free(h);
    h = next;
  }
}

static void free_out_list(h2_out_t *m) {
  while (m != NULL) {
    h2_out_t *next = m->next;
    free(m->payload);
    h2_node_free(m);
    m = next;
  }
}

static void out_release(h2_out_t *m) {
  free(m->payload);
  h2_node_free(m);
}
```

### Trailers

Trailers are copied into module-owned storage, submitted through nghttp2, and released when the stream slot is destroyed.

```c
static int h2_trailers_store(http2_stream_t *st, const nghttp2_nv *nva,
                             size_t nvlen) {
  st->trailer_nva = NULL;
  st->trailer_nvlen = 0;
  if (nvlen == 0) {
    return 0;
  }
  nghttp2_nv *cp = calloc(nvlen, sizeof(*cp));
  if (cp == NULL) {
    return -1;
  }
  for (size_t i = 0; i < nvlen; i++) {
    size_t blen = nva[i].namelen + nva[i].valuelen;
    uint8_t *blk = malloc(blen > 0 ? blen : 1);
    if (blk == NULL) {
      for (size_t j = 0; j < i; j++) {
        free(cp[j].name);
      }
      free(cp);
      return -1;
    }
    memcpy(blk, nva[i].name, nva[i].namelen);
    memcpy(blk + nva[i].namelen, nva[i].value, nva[i].valuelen);
    cp[i].name = blk;
    cp[i].namelen = nva[i].namelen;
    cp[i].value = blk + nva[i].namelen;
    cp[i].valuelen = nva[i].valuelen;
    cp[i].flags = NGHTTP2_NV_FLAG_NONE;
  }
  st->trailer_nva = cp;
  st->trailer_nvlen = (uint32_t)nvlen;
  return 0;
}

static void h2_trailers_free(http2_stream_t *st) {
  if (st->trailer_nva != NULL) {
    for (uint32_t i = 0; i < st->trailer_nvlen; i++) {
      free(st->trailer_nva[i].name);
    }
    free(st->trailer_nva);
    st->trailer_nva = NULL;
  }
  st->trailer_nvlen = 0;
}

static int h2_trailers_submit(http2_session_t *s, http2_stream_t *st) {
  if (nghttp2_submit_trailer(s->session, st->id, st->trailer_nva,
                             st->trailer_nvlen) != 0) {
    log_error("[h2] submit_trailer failed sid=%d", (int)st->id);
    h2_trailers_free(st);
    st->trailers = 0;
    return -1;
  }
  h2_trailers_free(st); /* nghttp2 copied the fields */
  st->trailers = 2;
  st->resp_closed = 1;
  return 0;
}
```

### Slot table and stream lifetime

The slot table maps stream IDs to per-stream state. It grows by rehashing, supports O(1)-ish lookup/erase, and owns stream cleanup. `flush_batch()` delivers a completed batch once and releases residual references. `on_stream_close()` is the nghttp2 callback that tears down the slot.

```c
static inline uint32_t slot_hash(int32_t id, uint32_t cap) {
  return ((uint32_t)id * 2654435761u) & (cap - 1);
}

static int slot_grow(http2_session_t *s) {
  uint32_t newcap = s->slot_cap != 0 ? s->slot_cap * 2 : 16;
  http2_stream_t **ns = calloc(newcap, sizeof(*ns));
  if (ns == NULL) {
    return -1;
  }
  for (uint32_t k = 0; k < s->slot_cap; k++) {
    http2_stream_t *st = s->slots[k];
    if (st == NULL) {
      continue;
    }
    uint32_t i = slot_hash(st->id, newcap);
    while (ns[i] != NULL) {
      i = (i + 1) & (newcap - 1);
    }
    ns[i] = st;
  }
  free(s->slots);
  s->slots = ns;
  s->slot_cap = newcap;
  return 0;
}

static http2_stream_t *slot_find(http2_session_t *s, int32_t id) {
  if (s->slot_cap == 0) {
    return NULL;
  }
  uint32_t mask = s->slot_cap - 1;
  uint32_t i = slot_hash(id, s->slot_cap);
  while (s->slots[i] != NULL) {
    if (s->slots[i]->id == id) {
      return s->slots[i];
    }
    i = (i + 1) & mask;
  }
  return NULL;
}

static int slot_insert(http2_session_t *s, http2_stream_t *st) {
  if ((s->slot_used + 1) * 4 >= s->slot_cap * 3) {
    if (slot_grow(s) != 0) {
      return -1;
    }
  }
  uint32_t mask = s->slot_cap - 1;
  uint32_t i = slot_hash(st->id, s->slot_cap);
  while (s->slots[i] != NULL) {
    i = (i + 1) & mask;
  }
  s->slots[i] = st;
  s->slot_used++;
  return 0;
}

static void slot_erase(http2_session_t *s, int32_t id) {
  if (s->slot_cap == 0) {
    return;
  }
  uint32_t cap = s->slot_cap, mask = cap - 1;
  uint32_t i = slot_hash(id, cap);
  while (s->slots[i] != NULL && s->slots[i]->id != id) {
    i = (i + 1) & mask;
  }
  if (s->slots[i] == NULL) {
    return;
  }
  s->slot_used--;
  /* Backward-shift: pull forward the run of entries whose probe path would
   * otherwise be broken by the hole at i. */
  for (;;) {
    s->slots[i] = NULL;
    uint32_t j = i;
    for (;;) {
      j = (j + 1) & mask;
      if (s->slots[j] == NULL) {
        return;
      }
      uint32_t home = slot_hash(s->slots[j]->id, cap);
      int in_range =
          (i <= j) ? (i < home && home <= j) : (i < home || home <= j);
      if (!in_range) {
        break;
      }
    }
    s->slots[i] = s->slots[j];
    i = j;
  }
}

static void stream_free_node(http2_session_t *s, http2_stream_t *dead) {
  h2_uncharge_unary(s, dead);
  /* The parked in-flight frame (if any) is still linked in dead->out_head, so
   * clear the parking slot first; free_out_list() then releases it exactly
   * once. Clearing without freeing avoids a double-free. */
  if (s->parked.active && s->parked.msg != NULL && s->parked.sid == dead->id) {
    s->parked.active = 0;
    s->parked.msg = NULL;
  }
  free_headers(dead->headers);
  free(dead->params);
  free(dead->body);
  h2_trailers_free(dead);
  free_out_list(dead->out_head);
  if (dead->ctx != NULL && dead->route != NULL) {
    /* Run the route's destructor. Only module-allocated state (state_size > 0)
     * is returned to the pool; state_size == 0 means app-managed ctx. */
    if (dead->route->state_free != NULL) {
      dead->route->state_free(dead->ctx);
    }
    if (dead->route->state_size > 0) {
      h2_small_free(dead->ctx, dead->route->state_size);
    }
  }
  h2_node_free(dead);
}

void http2_rst_stream(http2_session_t *s, int32_t sid) {
  if (s == NULL) {
    return;
  }
  h2_require_owner(s);
  nghttp2_submit_rst_stream(s->session, 0, sid, NGHTTP2_PROTOCOL_ERROR);
}

static int stream_reserve(http2_stream_t *st, size_t want) {
  if (want <= st->body_cap) {
    return 0;
  }
  size_t cap = st->body_cap != 0 ? st->body_cap : 256;
  while (cap < want) {
    if (cap > SIZE_MAX / 2) {
      cap = want;
      break;
    }
    cap *= 2;
  }
  uint8_t *grown = realloc(st->body, cap);
  if (grown == NULL) {
    return -1;
  }
  st->body = grown;
  st->body_cap = cap;
  return 0;
}

static int h2_consume_stream(http2_session_t *s, int32_t sid, size_t n) {
  if (n == 0) {
    return 0;
  }
  /* A detached batch can outlive its stream (or the app can consume after a
   * reset). The stream-level window is irrelevant then, but the connection
   * window was still debited for those bytes, so credit it directly. */
  if (slot_find(s, sid) != NULL &&
      nghttp2_session_consume(s->session, sid, n) == 0) {
    return 0;
  }
  if (nghttp2_session_consume_connection(s->session, n) != 0) {
    log_error("[h2] consume failed sid=%d n=%zu", (int)sid, n);
    return -1;
  }
  return 0;
}

static size_t flush_batch(http2_session_t *s, http2_stream_t *st,
                          http2_msg_t *tmp, size_t bn, size_t consume) {
  if (bn == 0) {
    h2_consume_stream(s, st->id, consume);
    return 0;
  }
  http2_stream_fn sink = st->handler.stream;
  if (!st->detach) {
    if (sink != NULL) {
      /* Attached: credit is the module's job, not the app's. */
      http2_batch_t b = {tmp, bn, 0, st->id, 0};
      sink(s, st, &b);
    }
    h2_consume_stream(s, st->id, consume);
    return 0;
  }

  size_t total = 0;
  for (size_t i = 0; i < bn; i++) {
    total += tmp[i].len;
  }
  size_t hdr = sizeof(http2_batch_t) + bn * sizeof(http2_msg_t);
  uint8_t *base = malloc(hdr + total);
  if (base == NULL) {
    h2_consume_stream(s, st->id, consume);
    http2_rst_stream(s, st->id);
    return 0;
  }
  http2_batch_t *b = (http2_batch_t *)base;
  b->msgs = (http2_msg_t *)(base + sizeof(http2_batch_t));
  b->n = bn;
  b->owned = 1;
  b->sid = st->id;
  b->consume = consume;
  uint8_t *dst = base + hdr;
  for (size_t i = 0; i < bn; i++) {
    b->msgs[i].msg = dst;
    b->msgs[i].len = tmp[i].len;
    memcpy(dst, tmp[i].msg, tmp[i].len);
    dst += tmp[i].len;
  }
  if (sink != NULL) {
    sink(s, st, b); /* ownership passes to the handler */
    return consume; /* app credits it via http2_batch_consume() */
  }
  h2_consume_stream(s, st->id, consume);
  http2_batch_release(b); /* no handler: do not leak */
  return 0;
}

static int on_stream_close(nghttp2_session *session, int32_t stream_id,
                           uint32_t error_code, void *user_data) {
  (void)session;
  (void)error_code;
  free_stream(user_data, stream_id);
  return 0;
}
```

### Session lifecycle and control

The session constructor installs the nghttp2 callbacks, windows, and manual flow-control options. The control functions cover graceful shutdown, GOAWAY, PING, budget inspection, activity tracking, and route handler classification.

```c
static http2_session_t *http2_session_new_impl(
    const http2_transport_t *transport, int client_fd,
    uint32_t stream_window_size, uint32_t connection_window_size) {
  if (stream_window_size == 0 || connection_window_size == 0) {
    return NULL;
  }
  if (stream_window_size > (uint32_t)NGHTTP2_MAX_WINDOW_SIZE) {
    stream_window_size = (uint32_t)NGHTTP2_MAX_WINDOW_SIZE;
  }
  if (connection_window_size > (uint32_t)NGHTTP2_MAX_WINDOW_SIZE) {
    connection_window_size = (uint32_t)NGHTTP2_MAX_WINDOW_SIZE;
  }

  http2_session_t *s = calloc(1, sizeof(*s));
  if (s == NULL) {
    return NULL;
  }
  s->fd = client_fd;
  s->owner = pthread_self();
  s->stream_window = stream_window_size;
  if (transport != NULL) {
    s->write_fn = transport->write;
    s->writev_fn = transport->writev;
    s->transport_ctx = transport->ctx;
  }

  nghttp2_session_callbacks *cbs = NULL;
  if (nghttp2_session_callbacks_new(&cbs) != 0) {
    free(s);
    return NULL;
  }
  nghttp2_session_callbacks_set_send_callback(cbs, on_send);
  nghttp2_session_callbacks_set_send_data_callback(cbs, on_send_data);
  nghttp2_session_callbacks_set_data_source_read_length_callback(
      cbs, on_read_length);
  nghttp2_session_callbacks_set_on_frame_recv_callback(cbs, on_frame_recv);
  nghttp2_session_callbacks_set_on_data_chunk_recv_callback(cbs,
                                                            on_data_chunk);
  nghttp2_session_callbacks_set_on_stream_close_callback(cbs, on_stream_close);
  nghttp2_session_callbacks_set_on_header_callback(cbs, on_header);

  /* Manual flow-control accounting: the module credits bytes as it buffers or
   * delivers them, and leaves detached batches uncredited until the consumer
   * reports progress (http2_batch_consume). Until then the peer throttles on
   * the stream/connection window -- that is the backpressure. */
  nghttp2_option *opt = NULL;
  if (nghttp2_option_new(&opt) != 0) {
    nghttp2_session_callbacks_del(cbs);
    free(s);
    return NULL;
  }
  nghttp2_option_set_no_auto_window_update(opt, 1);
  int srv = nghttp2_session_server_new2(&s->session, cbs, s, opt);
  nghttp2_option_del(opt);
  if (srv != 0) {
    nghttp2_session_callbacks_del(cbs);
    free(s);
    return NULL;
  }
  nghttp2_session_callbacks_del(cbs);

  /* Emit the server connection preface (SETTINGS) right away, advertising
   * sane bounds: a concurrency cap and a header-list cap. The receive windows
   * are raised separately (see http2_session_new). */
  const nghttp2_settings_entry sv[] = {
      {NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, 128},
      {NGHTTP2_SETTINGS_MAX_HEADER_LIST_SIZE, 65536},
  };
  if (nghttp2_submit_settings(s->session, 0, sv,
                              sizeof(sv) / sizeof(sv[0])) != 0) {
    nghttp2_session_del(s->session);
    free(s);
    return NULL;
  }
  /* Raise the connection-level receive window too; the peer sees a WINDOW_UPDATE
   * alongside the SETTINGS. */
  if (nghttp2_session_set_local_window_size(
          s->session, NGHTTP2_FLAG_NONE, 0,
          (int32_t)connection_window_size) != 0) {
    nghttp2_session_del(s->session);
    free(s);
    return NULL;
  }
  if (nghttp2_session_send(s->session) != 0) {
    nghttp2_session_del(s->session);
    free(s);
    return NULL;
  }
  return s;
}

http2_session_t *http2_session_new(int client_fd,
                                   uint32_t stream_window_size,
                                   uint32_t connection_window_size) {
  return http2_session_new_impl(NULL, client_fd, stream_window_size,
                                connection_window_size);
}

http2_session_t *http2_session_new_transport(const http2_transport_t *transport,
                                             int client_fd,
                                             uint32_t stream_window_size,
                                             uint32_t connection_window_size) {
  if (transport == NULL || transport->write == NULL ||
      transport->writev == NULL) {
    return NULL;
  }
  return http2_session_new_impl(transport, client_fd, stream_window_size,
                                connection_window_size);
}

void http2_session_free(http2_session_t *s) {
  if (s == NULL) {
    return;
  }
  h2_require_owner(s);
  for (uint32_t i = 0; i < s->slot_cap; i++) {
    if (s->slots[i] != NULL) {
      stream_free_node(s, s->slots[i]);
      s->slots[i] = NULL;
    }
  }
  free(s->slots);
  if (s->parked.active && s->parked.msg != NULL) {
    /* Session dying with a frame mid-flight (idle connection closed
     * between EAGAIN and resume): release it here. */
    out_release(s->parked.msg);
  }
  nghttp2_session_del(s->session);
  free(s);
}

int http2_session_shutdown_notice(http2_session_t *s) {
  if (s == NULL) {
    return -1;
  }
  h2_require_owner(s);
  if (nghttp2_submit_shutdown_notice(s->session) != 0) {
    return -1;
  }
  return http2_flush(s);
}

int http2_session_goaway(http2_session_t *s, uint32_t error_code) {
  if (s == NULL) {
    return -1;
  }
  h2_require_owner(s);
  int32_t last = nghttp2_session_get_last_proc_stream_id(s->session);
  if (nghttp2_submit_goaway(s->session, NGHTTP2_FLAG_NONE, last, error_code,
                            NULL, 0) != 0) {
    return -1;
  }
  return http2_flush(s);
}

size_t http2_session_active_streams(const http2_session_t *s) {
  h2_require_owner(s);
  return s != NULL ? s->slot_used : 0;
}

int http2_session_ping(http2_session_t *s, uint64_t opaque) {
  if (s == NULL) {
    return -1;
  }
  h2_require_owner(s);
  uint8_t data[8];
  memcpy(data, &opaque, sizeof data);
  return nghttp2_submit_ping(s->session, NGHTTP2_FLAG_NONE, data) == 0 ? 0
                                                                       : -1;
}

int http2_session_ping_ack(http2_session_t *s, uint64_t *opaque) {
  if (s == NULL) {
    return 0;
  }
  h2_require_owner(s);
  if (!s->ping_ack_seen) {
    return 0;
  }
  s->ping_ack_seen = 0;
  if (opaque != NULL) {
    *opaque = s->ping_ack_opaque;
  }
  return 1;
}

void http2_set_global_unary_budget(size_t bytes) {
  atomic_store_explicit(&g_unary_budget, bytes, memory_order_relaxed);
}

int http2_session_set_unary_budget(http2_session_t *s, size_t bytes) {
  if (s == NULL) {
    return -1;
  }
  h2_require_owner(s);
  s->unary_budget = bytes;
  return 0;
}

size_t http2_session_unary_bytes(const http2_session_t *s) {
  if (s == NULL) {
    return 0;
  }
  h2_require_owner(s);
  return s->unary_bytes;
}

uint64_t http2_session_app_activity(const http2_session_t *s) {
  if (s == NULL) {
    return 0;
  }
  h2_require_owner(s);
  return s->app_activity;
}

static void http2_stream_set_unary(http2_stream_t *st, http2_msg_framing_cb msg_framing,
                            const void *cfg, http2_unary_fn fn) {
  if (st == NULL) {
    return;
  }
  st->mode = HTTP2_UNARY;
  st->msg_framing = msg_framing;
  st->framing_cfg = cfg;
  st->prefix_len = 0;
  st->prefix_got = 0;
  st->handler.unary = fn;
}

static void http2_stream_set_stream(http2_stream_t *st, size_t prefix_len,
                                    http2_msg_framing_cb msg_framing,
                                    const void *cfg, http2_stream_fn fn) {
  if (st == NULL) {
    return;
  }
  /* Streaming needs a framing rule; without one there are no message
   * boundaries, so fall back to unary buffering. */
  st->mode = msg_framing != NULL ? HTTP2_STREAMING : HTTP2_UNARY;
  st->msg_framing = msg_framing;
  st->framing_cfg = cfg;
  st->prefix_len = st->mode == HTTP2_STREAMING ? prefix_len : 0;
  st->prefix_got = 0;
  st->handler.stream = fn;
}

static void http2_stream_set_detach(http2_stream_t *st, int detach) {
  if (st != NULL) {
    st->detach = detach != 0;
  }
}
```

### Public response and query API

These are the application-facing accessors and response builders. They validate ownership and state, copy trailers where required, and return borrowed views only for the duration documented by the caller contract.

```c
const uint8_t *http2_prefix(const http2_stream_t *st, size_t *len) {
  if (st == NULL || st->prefix_len == 0) {
    if (len != NULL) {
      *len = 0;
    }
    return NULL;
  }
  if (len != NULL) {
    *len = st->prefix_len;
  }
  return st->params;
}

static http2_stream_t *http2_find_stream(http2_session_t *s, int32_t id) {
  if (s == NULL) {
    return NULL;
  }
  return slot_find(s, id);
}

const uint8_t *http2_header_get(const http2_stream_t *st, const char *name,
                                size_t namelen, size_t *vlen) {
  if (st == NULL) {
    return NULL;
  }
  for (const http2_header_t *h = st->headers; h != NULL; h = h->next) {
    if (h->namelen == namelen && memcmp(h->name, name, namelen) == 0) {
      if (vlen != NULL) {
        *vlen = h->valuelen;
      }
      return h->value;
    }
  }
  return NULL;
}

static int http2_respond_headers(http2_session_t *s, int32_t sid,
                                 const nghttp2_nv *nva, size_t nvlen) {
  if (s == NULL) {
    return -1;
  }
  nghttp2_data_provider prd;
  prd.source.ptr = (void *)(intptr_t)sid; /* read resolves the live stream */
  prd.read_callback = on_data_read;
  /* One response submission per stream: the provider drains the stream's
   * queue across as many DATA frames as needed (see on_data_read). */
  if (nghttp2_submit_response(http2_raw(s), sid, nva, nvlen, &prd) != 0) {
    log_error("[h2] submit_response failed sid=%d", (int)sid);
    return -1;
  }
  return 0;
}

int http2_respond_begin(http2_session_t *s, int32_t sid, int status,
                        const nghttp2_nv *headers, size_t nheaders) {
  if (s == NULL || sid <= 0 || status < 100 || status > 599 ||
      (nheaders > 0 && headers == NULL)) {
    return -1;
  }
  h2_require_owner(s);
  http2_stream_t *st = http2_find_stream(s, sid);
  if (st == NULL) {
    return -1; /* unknown or already closed */
  }
  if (st->resp_opened || st->resp_closed || st->trailers != 0) {
    log_error("[h2] respond_begin: stream %d already answered", (int)sid);
    return -1;
  }
  /* HTTP status is exactly 3 digits; nghttp2 copies the value during submit,
   * so this stack buffer is fine. */
  char statusbuf[4];
  statusbuf[0] = (char)('0' + (status / 100) % 10);
  statusbuf[1] = (char)('0' + (status / 10) % 10);
  statusbuf[2] = (char)('0' + status % 10);
  statusbuf[3] = '\0';
  /* Common case is status-only (nheaders==0): avoid a heap allocation. */
  nghttp2_nv stack[8];
  nghttp2_nv *nva = stack;
  if (nheaders + 1 > sizeof(stack) / sizeof(stack[0])) {
    nva = malloc((nheaders + 1) * sizeof(*nva));
    if (nva == NULL) {
      http2_rst_stream(s, sid);
      return -1;
    }
  }
  nva[0] = (nghttp2_nv){(uint8_t *)":status", (uint8_t *)statusbuf, 7, 3,
                        NGHTTP2_NV_FLAG_NONE};
  for (size_t i = 0; i < nheaders; i++) {
    nva[i + 1] = headers[i];
  }
  int rc = http2_respond_headers(s, sid, nva, nheaders + 1);
  if (nva != stack) {
    free(nva);
  }
  if (rc != 0) {
    http2_rst_stream(s, sid);
    return -1;
  }
  st->resp_opened = 1;
  return 0;
}

int http2_respond_msg(http2_session_t *s, int32_t sid, const uint8_t *body,
                      size_t len, int end_stream) {
  if (s == NULL || sid <= 0 || (len > 0 && body == NULL)) {
    return -1;
  }
  h2_require_owner(s);
  http2_stream_t *st = http2_find_stream(s, sid);
  if (st == NULL) {
    return -1; /* unknown or already closed */
  }
  if (st->resp_closed || st->trailers != 0) {
    log_error("[h2] respond_msg after EOF on stream %d", (int)sid);
    return -1;
  }
  if (!st->resp_opened) {
    if (len == 0 && end_stream) {
      /* Empty reply: a bare HEADERS with END_STREAM, no provider needed. */
      nghttp2_nv res[] = {
          {(uint8_t *)":status", (uint8_t *)"200", 7, 3, NGHTTP2_NV_FLAG_NONE}};
      if (nghttp2_submit_headers(http2_raw(s), NGHTTP2_FLAG_END_STREAM, sid,
                                 NULL, res, 1, NULL) != 0) {
        log_error("[h2] respond_msg: empty headers failed sid=%d", (int)sid);
        http2_rst_stream(s, sid);
        return -1;
      }
      st->resp_opened = 1;
      st->resp_closed = 1;
      return 0;
    }
    if (http2_respond_begin(s, sid, 200, NULL, 0) != 0) {
      return -1; /* respond_begin already reset the stream */
    }
  }
  if (len == 0) {
    /* No payload bytes: only meaningful to carry END_STREAM. */
    if (!end_stream) {
      return 0;
    }
    return http2_send(s, sid, NULL, 0, 1);
  }
  /* The body may point into borrowed assembly/chunk memory, so a copy is
   * transferred to http2_send (freed on full send or reset). */
  uint8_t *copy = malloc(len);
  if (copy == NULL) {
    http2_rst_stream(s, sid);
    return -1;
  }
  memcpy(copy, body, len);
  if (http2_send(s, sid, copy, len, end_stream) != 0) {
    http2_rst_stream(s, sid);
    return -1;
  }
  return 0;
}

int http2_respond_end(http2_session_t *s, int32_t sid) {
  return http2_respond_msg(s, sid, NULL, 0, 1);
}

int http2_respond_trailers(http2_session_t *s, int32_t sid,
                           const nghttp2_nv *nva, size_t nvlen) {
  if (s == NULL || sid <= 0 || (nvlen > 0 && nva == NULL)) {
    return -1;
  }
  h2_require_owner(s);
  http2_stream_t *st = http2_find_stream(s, sid);
  if (st == NULL) {
    return -1;
  }
  if (st->resp_closed || st->trailers != 0) {
    log_error("[h2] respond_trailers: stream %d already finished", (int)sid);
    return -1;
  }
  if (!st->resp_opened) {
    if (http2_respond_begin(s, sid, 200, NULL, 0) != 0) {
      return -1;
    }
  }
  if (st->out_head == NULL) {
    /* Give the data provider a zero-length final node: it signals EOF so the
     * trailer HEADERS is ordered after a DATA frame. */
    if (http2_send(s, sid, NULL, 0, 0) != 0) {
      http2_rst_stream(s, sid);
      return -1;
    }
  }
  if (h2_trailers_store(st, nva, nvlen) != 0) {
    http2_rst_stream(s, sid);
    return -1;
  }
  st->trailers = 1;
  return 0;
}

int http2_respond_msg_owned(http2_session_t *s, int32_t sid, uint8_t *body,
                            size_t len, int end_stream) {
  if (s == NULL || sid <= 0 || (len > 0 && body == NULL)) {
    free(body);
    return -1;
  }
  h2_require_owner(s);
  http2_stream_t *st = http2_find_stream(s, sid);
  if (st == NULL) {
    free(body); /* unknown or already closed */
    return -1;
  }
  if (st->resp_closed || st->trailers != 0) {
    log_error("[h2] respond_msg_owned after EOF on stream %d", (int)sid);
    free(body);
    return -1;
  }
  if (len == 0) {
    /* Nothing to carry: an owned pointer has no bytes to own. */
    free(body);
    return http2_respond_msg(s, sid, NULL, 0, end_stream);
  }
  if (!st->resp_opened) {
    if (http2_respond_begin(s, sid, 200, NULL, 0) != 0) {
      free(body); /* respond_begin did not take ownership */
      return -1;
    }
  }
  /* Ownership of `body` transfers to http2_send: it is freed after the final
   * byte goes out, when the queued node is dropped on reset/close, or here if
   * the submission itself fails. */
  if (http2_send(s, sid, body, len, end_stream) != 0) {
    http2_rst_stream(s, sid);
    return -1;
  }
  return 0;
}

static int http2_send(http2_session_t *s, int32_t sid, uint8_t *payload,
                      size_t len, int end_stream) {
  if (s == NULL || sid <= 0 || (len > 0 && payload == NULL)) {
    free(payload);
    return -1;
  }
  http2_stream_t *st = http2_find_stream(s, sid);
  if (st == NULL) {
    free(payload); /* peer never opened it (or already closed) */
    return -1;
  }
  if (st->resp_closed || st->trailers != 0) {
    log_error("[h2] send after EOF on stream %d", (int)sid);
    free(payload);
    return -1;
  }
  /* One node per payload, any size: the read_length callback below chunks
   * it into window-sized frames at send time (nghttp2 will not fragment
   * NO_COPY payloads itself). Ownership transfers to the queue. */
  h2_out_t *m = h2_node_alloc();
  if (m == NULL) {
    free(payload);
    return -1;
  }
  m->payload = payload;
  m->len = len;
  m->eof = end_stream ? 1 : 0;
  m->next = NULL;
  if (st->out_tail != NULL) {
    st->out_tail->next = m;
  } else {
    st->out_head = m;
  }
  st->out_tail = m;
  if (st->data_deferred) {
    /* A read deferred earlier on an empty queue: wake it so the newly
     * appended payload actually sends. (Without this, nothing re-fires
     * the read and the payload would stall until the next flush cycle.) */
    st->data_deferred = 0;
    if (nghttp2_session_resume_data(s->session, sid) != 0) {
      log_error("[h2] resume_data sid=%d failed", (int)sid);
    }
  }
  /* No auto-flush: feed()'s trailing send covers in-callback appends;
   * otherwise the caller follows with http2_flush(). */
  return 0;
}

nghttp2_session *http2_raw(http2_session_t *s) {
  if (s == NULL) {
    return NULL;
  }
  h2_require_owner(s);
  return s->session;
}

size_t http2_batch_consume_bytes(const http2_batch_t *b) {
  return b != NULL ? b->consume : 0;
}

int http2_stream_consume(http2_session_t *s, int32_t sid, size_t n) {
  if (s == NULL || sid <= 0) {
    return -1;
  }
  h2_require_owner(s);
  if (h2_consume_stream(s, sid, n) != 0) {
    return -1;
  }
  return http2_flush(s);
}

int http2_batch_consume(http2_session_t *s, http2_batch_t *b) {
  if (s == NULL || b == NULL || b->consume == 0) {
    return 0;
  }
  h2_require_owner(s);
  int rc = h2_consume_stream(s, b->sid, b->consume);
  if (rc == 0) {
    b->consume = 0; /* idempotent: never credit the same bytes twice */
  }
  if (rc != 0) {
    return -1;
  }
  return http2_flush(s);
}
```

### Stream accessors

```c
int32_t http2_stream_id(const http2_stream_t *st) {
  return st != NULL ? st->id : 0;
}

void *http2_stream_ctx(const http2_stream_t *st) {
  return st != NULL ? st->ctx : NULL;
}

const http2_header_t *http2_stream_headers(const http2_stream_t *st) {
  return st != NULL ? st->headers : NULL;
}
```

### Coverage rule

Every function in `src/http2/http2.c` is now represented in a code block in this
document. When adding a new internal function, add its definition to the phase
where its behavior is introduced, or to this companion if it is supporting
plumbing. Do not add full-file dumps; keep the snippets grouped by subsystem and
anchor them to the ownership/flow-control explanation above.

## What is intentionally out of scope

This layer is not a complete HTTP server. It does not do TLS, HTTP/1.1 upgrade,
ALPN negotiation, server push, priority scheduling, content compression, or
application routing policy. It does not decide timeouts, keepalive intervals, or
connection limits. Those belong to the server that embeds it.

What it does provide is the hard part that is easy to get wrong: HTTP/2 framing
integration, request lifetime, body assembly, backpressure, zero-copy replies,
and a small API that a worker thread can drive without blocking.

The final mental model is:

> nghttp2 owns the protocol. The HTTP/2 layer owns the connection's request and
> reply state. The async engine owns the socket and the thread. The application
> owns routes, handlers, and policy.

Once those boundaries are clear, the public header is just the contract between
them.
