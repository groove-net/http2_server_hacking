#include "async_engine.h"
#include "config.h"
#include "http2_server.h"
#include "liblog.h"
#include "tls_server.h"
#include <limits.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <nghttp2/nghttp2.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>

static _Atomic int g_conn_count = 0;
static _Atomic int g_shutdown = 0;

static int g_max_conn = 10000;
static uint64_t g_idle_ms = 30000;
static uint64_t g_drain_notice_ms = 500;
static uint64_t g_drain_timeout_ms = 10000;
static uint64_t g_keepalive_ms = 0;
static uint64_t g_keepalive_timeout_ms = 10000;
static uint32_t g_stream_window = HTTP2_DEFAULT_WINDOW_SIZE;
static uint32_t g_connection_window = 4194304;
static tls_config_t *g_tls_config = NULL;

typedef struct client_ctx {
  http2_session_t *h2;
  tls_conn_t *tls;
  uint8_t *plain_buffer;
  size_t plain_size;
  int tls_ready;
  io_task_t *task;
  uint64_t last_activity_ms;
  uint64_t last_app_activity_ms;
  uint64_t app_activity;
  uint64_t ping_sent_ms;
  uint64_t ping_opaque;
  int ping_pending;
  int draining;
  io_timer_t *idle_timer;
  struct client_ctx *next;
  struct client_ctx *prev;
} client_ctx_t;

static _Thread_local client_ctx_t *tl_clients = NULL;
static _Thread_local io_task_t *tl_accept_task = NULL;
static _Thread_local int tl_draining = 0;
static _Thread_local uint64_t tl_drain_notice_at = 0;
static _Thread_local uint64_t tl_drain_deadline = 0;
static _Thread_local int tl_drain_final_sent = 0;
static _Thread_local uint64_t tl_ping_seq = 0;
static _Thread_local io_timer_t *tl_house_timer = NULL;

static io_loop_t *g_loops[THREAD_COUNT];
static pthread_mutex_t g_loops_mu = PTHREAD_MUTEX_INITIALIZER;

static uint64_t now_ms(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000u + (uint64_t)(ts.tv_nsec / 1000000L);
}

static uint64_t env_u64(const char *name, uint64_t def) {
  const char *v = getenv(name);
  if (v == NULL || *v == '\0') {
    return def;
  }
  char *end = NULL;
  unsigned long long n = strtoull(v, &end, 10);
  if (end == v) {
    return def;
  }
  return (uint64_t)n;
}

static uint32_t env_window(const char *name, uint32_t def) {
  uint64_t v = env_u64(name, def);
  if (v > (uint64_t)NGHTTP2_MAX_WINDOW_SIZE) {
    v = (uint64_t)NGHTTP2_MAX_WINDOW_SIZE;
  }
  return (uint32_t)v;
}

void server_limits_init(void) {
  uint64_t mc = env_u64("H2_MAX_CONNECTIONS", 10000);
  g_max_conn = mc > (uint64_t)INT_MAX ? INT_MAX : (int)mc;
  g_idle_ms = env_u64("H2_IDLE_TIMEOUT_MS", 30000);
  g_drain_notice_ms = env_u64("H2_DRAIN_NOTICE_MS", 500);
  g_drain_timeout_ms = env_u64("H2_DRAIN_TIMEOUT_MS", 10000);
  g_keepalive_ms = env_u64("H2_KEEPALIVE_MS", 0);
  g_keepalive_timeout_ms = env_u64("H2_KEEPALIVE_TIMEOUT_MS", 10000);
  if (g_keepalive_timeout_ms == 0) {
    g_keepalive_timeout_ms = g_keepalive_ms;
  }
  g_stream_window =
      env_window("H2_INITIAL_WINDOW_SIZE", HTTP2_DEFAULT_WINDOW_SIZE);
  g_connection_window = env_window("H2_CONN_WINDOW_SIZE", 4194304);
}

int server_tls_init(void) {
  const char *cert = getenv("H2_TLS_CERT");
  const char *key = getenv("H2_TLS_KEY");
  if (cert == NULL && key == NULL) {
    return 0;
  }
  if (cert == NULL || key == NULL) {
    log_error("[tls] H2_TLS_CERT and H2_TLS_KEY must both be set");
    return -1;
  }
  g_tls_config = tls_config_new(cert, key);
  if (g_tls_config == NULL) {
    log_error("[tls] failed to initialize TLS context");
    return -1;
  }
  log_info("[tls] enabled for new connections");
  return 1;
}

void server_tls_free(void) {
  tls_config_free(g_tls_config);
  g_tls_config = NULL;
}

int client_conn_admit(int fd) {
  if (g_max_conn <= 0) {
    atomic_fetch_add_explicit(&g_conn_count, 1, memory_order_relaxed);
    return 1;
  }
  int cur = atomic_load_explicit(&g_conn_count, memory_order_relaxed);
  for (;;) {
    if (cur >= g_max_conn) {
      close(fd);
      return 0;
    }
    if (atomic_compare_exchange_weak_explicit(&g_conn_count, &cur, cur + 1,
                                              memory_order_relaxed,
                                              memory_order_relaxed)) {
      return 1;
    }
  }
}

static void client_conn_release(void) {
  atomic_fetch_sub_explicit(&g_conn_count, 1, memory_order_relaxed);
}

void server_register_loop(io_loop_t *loop) {
  pthread_mutex_lock(&g_loops_mu);
  for (int i = 0; i < THREAD_COUNT; i++) {
    if (g_loops[i] == NULL) {
      g_loops[i] = loop;
      break;
    }
  }
  pthread_mutex_unlock(&g_loops_mu);
}

void server_unregister_loop(io_loop_t *loop) {
  pthread_mutex_lock(&g_loops_mu);
  for (int i = 0; i < THREAD_COUNT; i++) {
    if (g_loops[i] == loop) {
      g_loops[i] = NULL;
      break;
    }
  }
  pthread_mutex_unlock(&g_loops_mu);
}

void server_request_shutdown(void) {
  atomic_store_explicit(&g_shutdown, 1, memory_order_relaxed);
  pthread_mutex_lock(&g_loops_mu);
  for (int i = 0; i < THREAD_COUNT; i++) {
    if (g_loops[i] != NULL) {
      io_loop_request_stop(g_loops[i]);
    }
  }
  pthread_mutex_unlock(&g_loops_mu);
}

static void client_unlink(client_ctx_t *ctx) {
  if (ctx->prev != NULL) {
    ctx->prev->next = ctx->next;
  } else {
    tl_clients = ctx->next;
  }
  if (ctx->next != NULL) {
    ctx->next->prev = ctx->prev;
  }
}

static void client_idle_timer(void *arg) {
  client_ctx_t *ctx = arg;
  ctx->idle_timer = NULL;
  if (ctx->task == NULL || is_task_cancel_requested(ctx->task) ||
      ctx->draining) {
    return;
  }

  uint64_t now = now_ms();
  uint64_t idle = now - ctx->last_app_activity_ms;
  if (idle < g_idle_ms) {
    ctx->idle_timer = io_timer_add(io_loop_current(), g_idle_ms - idle,
                                   client_idle_timer, ctx);
    return;
  }

  if (ctx->h2 == NULL) {
    task_cancel(ctx->task);
    return;
  }
  if (http2_session_active_streams(ctx->h2) == 0 &&
      !nghttp2_session_want_write(http2_raw(ctx->h2))) {
    http2_session_goaway(ctx->h2, NGHTTP2_NO_ERROR);
    task_cancel(ctx->task);
    return;
  }

  ctx->idle_timer =
      io_timer_add(io_loop_current(), g_idle_ms, client_idle_timer, ctx);
}

static void server_tick(io_loop_t *loop) {
  uint64_t now = now_ms();

  if (!tl_draining &&
      (atomic_load_explicit(&g_shutdown, memory_order_relaxed) ||
       io_loop_stop_requested(loop))) {
    tl_draining = 1;
    tl_drain_notice_at = now;
    tl_drain_deadline = now + g_drain_timeout_ms;
    tl_drain_final_sent = 0;

    if (tl_accept_task != NULL) {
      task_cancel(tl_accept_task); // completion clears tl_accept_task
    }
    for (client_ctx_t *ctx = tl_clients; ctx != NULL; ctx = ctx->next) {
      ctx->draining = 1;
      if (ctx->h2 != NULL) {
        http2_session_shutdown_notice(ctx->h2);
      }
    }
  }

  if (tl_draining) {
    int notice_done =
        g_drain_notice_ms == 0 || now - tl_drain_notice_at >= g_drain_notice_ms;
    if (!tl_drain_final_sent && notice_done) {
      for (client_ctx_t *ctx = tl_clients; ctx != NULL; ctx = ctx->next) {
        if (ctx->h2 != NULL) {
          http2_session_goaway(ctx->h2, NGHTTP2_NO_ERROR);
        }
      }
      tl_drain_final_sent = 1;
    }

    client_ctx_t *ctx = tl_clients;
    while (ctx != NULL) {
      client_ctx_t *next = ctx->next;
      if (is_task_cancel_requested(ctx->task)) {
        ctx = next;
        continue; // completion is pending
      }
      int finish = 0;
      if (ctx->h2 == NULL) {
        finish = 1;
      } else {
        nghttp2_session *h = http2_raw(ctx->h2);
        int drained =
            !nghttp2_session_want_read(h) && !nghttp2_session_want_write(h);
        if (drained || now >= tl_drain_deadline) {
          finish = 1;
        } else if (http2_flush(ctx->h2) != 0) {
          finish = 1;
        }
      }
      if (finish) {
        task_cancel(ctx->task);
      }
      ctx = next;
    }

    if (tl_clients == NULL && tl_accept_task == NULL) {
      io_loop_exit(loop);
    }
    return;
  }

  if (g_keepalive_ms > 0) {
    client_ctx_t *ctx = tl_clients;
    while (ctx != NULL) {
      client_ctx_t *next = ctx->next;
      if (is_task_cancel_requested(ctx->task) || ctx->draining ||
          ctx->h2 == NULL) {
        ctx = next;
        continue;
      }
      if (ctx->ping_pending) {
        uint64_t ack = 0;
        if (http2_session_ping_ack(ctx->h2, &ack) && ack == ctx->ping_opaque) {
          ctx->ping_pending = 0;
        } else if (now - ctx->ping_sent_ms >= g_keepalive_timeout_ms) {
          task_cancel(ctx->task);
          ctx = next;
          continue;
        }
        if (ctx->ping_pending) {
          if (http2_flush(ctx->h2) != 0) {
            task_cancel(ctx->task);
            ctx = next;
            continue;
          }
        }
      }
      if (!ctx->ping_pending && now - ctx->last_activity_ms >= g_keepalive_ms) {
        uint64_t opaque = ++tl_ping_seq;
        if (http2_session_ping(ctx->h2, opaque) != 0 ||
            http2_flush(ctx->h2) != 0) {
          task_cancel(ctx->task);
          ctx = next;
          continue;
        }
        ctx->ping_pending = 1;
        ctx->ping_opaque = opaque;
        ctx->ping_sent_ms = now;
      }
      ctx = next;
    }
  }
}

static void server_house_timer(void *arg);

static void server_house_arm(io_loop_t *loop) {
  if (tl_house_timer == NULL && (tl_draining || g_keepalive_ms > 0)) {
    tl_house_timer = io_timer_add(loop, 100, server_house_timer, NULL);
  }
}

static void server_house_timer(void *arg) {
  (void)arg;
  tl_house_timer = NULL;
  io_loop_t *loop = io_loop_current();
  server_tick(loop);
  server_house_arm(loop);
}

static void server_stop(io_loop_t *loop) {
  server_tick(loop);
  server_house_arm(loop);
}

void my_mid_flight_handler(io_task_t *task);
void my_continuation_callback(void *context, int status, uint8_t *buffer);


static ssize_t client_tls_write(void *context, const uint8_t *buf, size_t len) {
  return tls_conn_write(context, buf, len);
}

static ssize_t client_tls_writev(void *context, const struct iovec *iov,
                                 int iovcnt) {
  return tls_conn_writev(context, iov, iovcnt);
}

static int client_tls_feed_encrypted(client_ctx_t *ctx, io_task_t *task) {
  if (task->bytes_buffered == 0) {
    return 0;
  }
  ctx->last_activity_ms = now_ms();
  size_t buffered = task->bytes_buffered;
  size_t span = task_read_span(task);
  const uint8_t *head = task->read_buffer + task->read_head;
  if (tls_conn_feed(ctx->tls, head, span) != (ssize_t)span ||
      (span < buffered &&
       tls_conn_feed(ctx->tls, task->read_buffer, buffered - span) !=
           (ssize_t)(buffered - span))) {
    return -1;
  }
  task_read_consume(task, buffered);
  return 0;
}

static int client_tls_read_plain(client_ctx_t *ctx) {
  for (;;) {
    ssize_t n = tls_conn_read(ctx->tls, ctx->plain_buffer, ctx->plain_size);
    if (n < 0) {
      return -1;
    }
    if (n == 0) {
      return 0;
    }
    if (http2_session_feed(ctx->h2, ctx->plain_buffer, (size_t)n) != 0) {
      return -1;
    }
  }
}

static int client_tls_flush(client_ctx_t *ctx) {
  return tls_conn_flush(ctx->tls) < 0 ? -1 : 0;
}

static void client_note_activity(client_ctx_t *ctx) {
  uint64_t app = http2_session_app_activity(ctx->h2);
  if (app != ctx->app_activity) {
    ctx->app_activity = app;
    ctx->last_app_activity_ms = now_ms();
  }
}

int setup_client_session_async(io_loop_t *loop, int client_fd) {
  int flags = fcntl(client_fd, F_GETFL, 0);
  if (flags < 0) {
    close(client_fd);
    return -1;
  }
  fcntl(client_fd, F_SETFL, flags | O_NONBLOCK);

  int one = 1;
  setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);

  client_ctx_t *ctx = calloc(1, sizeof *ctx);
  if (ctx == NULL) {
    close(client_fd);
    return -1;
  }
  io_task_t *task = task_create();
  if (task == NULL) {
    free(ctx);
    close(client_fd);
    return -1;
  }

  size_t read_size = 16384;
  uint8_t *read_buffer = malloc(read_size);
  if (read_buffer == NULL) {
    task_free(task);
    free(ctx);
    close(client_fd);
    return -1;
  }

  uint8_t *plain_buffer = NULL;
  tls_conn_t *tls = NULL;
  if (g_tls_config != NULL) {
    plain_buffer = malloc(read_size);
    tls = tls_conn_new(g_tls_config, client_fd);
    if (plain_buffer == NULL || tls == NULL) {
      tls_conn_free(tls);
      free(plain_buffer);
      free(read_buffer);
      task_free(task);
      free(ctx);
      close(client_fd);
      return -1;
    }
  }
  ctx->tls = tls;
  ctx->plain_buffer = plain_buffer;
  ctx->plain_size = read_size;

  task->fd = client_fd;
  task->state = TASK_ACTIVE;
  task->read_size = read_size;
  task->read_buffer = read_buffer;
  task->bytes_buffered = 0;
  task->write_buffer = NULL;
  task->write_size = 0;
  task->write_len = 0;
  task->write_off = 0;
  task->context = ctx;
  task->process_handler = my_mid_flight_handler;
  task->on_complete = my_continuation_callback;

  ctx->task = task;
  ctx->last_activity_ms = now_ms();
  ctx->last_app_activity_ms = ctx->last_activity_ms;

  if (io_loop_add_task(loop, task) != 0) {
    tls_conn_free(ctx->tls);
    free(ctx->plain_buffer);
    free(read_buffer);
    task_free(task);
    free(ctx);
    return -1;
  }
  if (task_start_read(task) != 0) {
    task->context = NULL;
    task->on_complete = NULL;
    task_cancel(task);
    tls_conn_free(ctx->tls);
    free(ctx->plain_buffer);
    free(read_buffer);
    free(ctx);
    return -1;
  }

  ctx->prev = NULL;
  ctx->next = tl_clients;
  if (tl_clients != NULL) {
    tl_clients->prev = ctx;
  }
  tl_clients = ctx;

  if (g_idle_ms > 0) {
    ctx->idle_timer = io_timer_add(loop, g_idle_ms, client_idle_timer, ctx);
  }

  return 0;
}

void my_mid_flight_handler(io_task_t *task) {
  client_ctx_t *ctx = task->context;
  if (ctx == NULL) {
    task->state = TASK_COMPLETED;
    return;
  }

  if (ctx->tls != NULL) {
    if (client_tls_feed_encrypted(ctx, task) != 0) {
      task->state = TASK_COMPLETED;
      return;
    }
    if (!ctx->tls_ready) {
      int hs = tls_conn_handshake(ctx->tls);
      if (hs < 0) {
        task->state = TASK_COMPLETED;
        return;
      }
      if (client_tls_flush(ctx) != 0) {
        task->state = TASK_COMPLETED;
        return;
      }
      if (!hs) {
        if (task->read_closed) {
          task->state = tls_conn_pending_out(ctx->tls) != 0 ? TASK_WAIT_WRITE
                                                            : TASK_COMPLETED;
        } else {
          task->state = tls_conn_pending_out(ctx->tls) != 0 ? TASK_WAIT_WRITE
                                                            : TASK_ACTIVE;
        }
        return;
      }
      ctx->tls_ready = 1;
    }
  }

  if (ctx->h2 == NULL) {
    if (ctx->tls != NULL) {
      http2_transport_t transport = {
          .ctx = ctx->tls,
          .write = client_tls_write,
          .writev = client_tls_writev,
      };
      ctx->h2 = http2_session_new_transport(
          &transport, task->fd, g_stream_window, g_connection_window);
    } else {
      ctx->h2 =
          http2_session_new(task->fd, g_stream_window, g_connection_window);
    }
    if (ctx->h2 == NULL) {
      task->state = TASK_COMPLETED;
      return;
    }
  }

  if (ctx->tls != NULL) {
    if (client_tls_read_plain(ctx) != 0) {
      task->state = TASK_COMPLETED;
      return;
    }
    client_note_activity(ctx);
  } else if (task->bytes_buffered > 0) {
    ctx->last_activity_ms = now_ms();
    size_t buffered = task->bytes_buffered;
    size_t span = task_read_span(task);
    const uint8_t *head = task->read_buffer + task->read_head;
    if (http2_session_feed(ctx->h2, head, span) != 0 ||
        (span < buffered && http2_session_feed(ctx->h2, task->read_buffer,
                                               buffered - span) != 0)) {
      task->state = TASK_COMPLETED;
      return;
    }
    client_note_activity(ctx);
    task_read_consume(task, buffered);
  }

  if (http2_flush(ctx->h2) != 0) {
    task->state = TASK_COMPLETED;
    return;
  }
  if (ctx->tls != NULL && client_tls_flush(ctx) != 0) {
    task->state = TASK_COMPLETED;
    return;
  }

  nghttp2_session *h = http2_raw(ctx->h2);
  int want_write = h != NULL && nghttp2_session_want_write(h) != 0;
  if (ctx->tls != NULL && tls_conn_pending_out(ctx->tls) != 0) {
    want_write = 1;
  }

  if (task->read_closed) {
    // The peer cannot send more input. Flush what is already queued, then
    // finish; if output is blocked, wait for writability first.
    task->state = want_write ? TASK_WAIT_WRITE : TASK_COMPLETED;
    return;
  }
  task->state = want_write ? TASK_WAIT_WRITE : TASK_ACTIVE;
}

void my_continuation_callback(void *context, int status, uint8_t *read_buffer) {
  (void)status;

  client_ctx_t *ctx = context;
  int had_ctx = ctx != NULL;
  if (ctx != NULL) {
    if (ctx->idle_timer != NULL) {
      io_timer_cancel(ctx->idle_timer);
    }
    if (ctx->h2 != NULL) {
      http2_session_free(ctx->h2);
    }
    if (ctx->tls != NULL) {
      tls_conn_shutdown(ctx->tls);
      tls_conn_flush(ctx->tls);
    }
    tls_conn_free(ctx->tls);
    free(ctx->plain_buffer);
    client_unlink(ctx);
  }
  free(read_buffer);
  free(ctx);
  if (had_ctx) {
    client_conn_release();
  }
}
