/*
 * HTTP/2 middleware demo.
 *
 * HTTP/2 exposes raw DATA events. The body middleware owns request buffering,
 * compression, limits, and lifecycle. The router selects a handler; the writer
 * owns response submission and quota accounting.
 */

#include "custring.h"
#include "http2_server.h"
#include "http_exchange.h"
#include "http_mw_logging.h"
#include "http_mw_router.h"
#include "http_mw_body.h"
#include "http_mw_writer.h"
#include "http_mw_compression.h"
#include "http_mw_body_limits.h"
#include "http_mw_queue_limits.h"
#include "liblog.h"
#include "server.h"
#include <arpa/inet.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>

/* ============================================================================
 * [ FRAMEWORK ] wire read helpers (fixed-width big-endian).
 * ========================================================================= */
static uint16_t rd16(const uint8_t *p) {
  uint16_t v;
  memcpy(&v, p, sizeof(v));
  return ntohs(v);
}

static uint32_t rd32(const uint8_t *p) {
  uint32_t v;
  memcpy(&v, p, sizeof(v));
  return ntohl(v);
}

/* ============================================================================
 * [ FRAMEWORK ] per-stream state + HTTP/2 plumbing.
 * ========================================================================= */

/* Per-stream application state. The route's state_size/state_free hooks own its
 * lifetime; the exchange and middleware state live here until stream close. */
typedef struct stream_state {
  const http2_route_t *route;
  int32_t sid;
  http_exchange_t *exchange;
} stream_state_t;

static void stream_state_free(void *p) {
  stream_state_t *ss = p;
  if (ss != NULL && ss->exchange != NULL) {
    http_exchange_close(ss->exchange);
    free(ss->exchange);
    ss->exchange = NULL;
  }
}

/* ============================================================================
 * ==============================  YOUR CODE  =================================
 * This project's handler and route. Wire integers are fixed-width big-endian;
 * strings are Pascal-style (u16-BE length + raw bytes).
 * ========================================================================= */

/* svc=0x03 method=0x07 Session.GetStats (example object):
 *   u32 status | u16 active | u16 idle | u8 flags | u16 name_len | name[]
 * fixed = 11; the Pascal name tail starts at offset 9. */
static void handle_getstats(http_exchange_t *x) {
  const uint8_t *p = x->req_body;
  uint32_t status = rd32(p + 0);
  uint16_t active = rd16(p + 4);
  uint16_t idle = rd16(p + 6);
  uint8_t flags = p[8];
  uint16_t nlen = rd16(p + 9);
  const uint8_t *name = p + 11;
  log_info("getstats status=%u active=%u idle=%u flags=%u name=%.*s", status,
           active, idle, flags, (int)nlen, (const char *)name);
  x->status = 200;
  x->resp_body = name;
  x->resp_body_len = nlen;
}

/* Session.GetStats response decoder for POST /3/7. */

static http_route_entry_t k_mw_routes[] = {
    {.method = "POST", .path = "/3/7", .handler = handle_getstats},
    {.method = "POST", .path = "/mw/echo", .handler = NULL},
    {.method = "POST", .path = "/mw/gzip", .handler = NULL,
     .compression = HTTP2_COMPRESSION_GZIP,
     .compression_profile = HTTP2_COMPRESSION_BALANCED,
     .max_decompressed = 1u << 20},
};
static http_mw_router_t k_router;
static http_stage_t k_stages[6];
static http_stage_t k_stream_stages[7];
static http_mw_body_t k_body_mw;
static http_mw_writer_t k_writer_mw;
static http_mw_compression_t k_compression_mw;
static http_mw_body_limits_t k_body_limits_mw;
static http_mw_queue_limits_t k_queue_limits_mw;

static void fill_exchange_meta(http_exchange_t *x, http2_stream_t *st);



static void fill_exchange_meta(http_exchange_t *x, http2_stream_t *st) {
  char *method = x->method_storage;
  char *path = x->path_storage;
  strcpy(method, "POST");
  const http2_header_t *hs = http2_stream_headers(st);
  for (const http2_header_t *h = hs; h != NULL; h = h->next) {
    if (h->namelen == 7 && memcmp(h->name, ":method", 7) == 0) {
      size_t n = h->valuelen < sizeof x->method_storage - 1
                     ? h->valuelen : sizeof x->method_storage - 1;
      memcpy(method, h->value, n); method[n] = '\0';
    } else if (h->namelen == 5 && memcmp(h->name, ":path", 5) == 0) {
      size_t n = h->valuelen < sizeof x->path_storage - 1
                     ? h->valuelen : sizeof x->path_storage - 1;
      memcpy(path, h->value, n); path[n] = '\0';
    }
  }
  x->method = method;
  x->path = path;
  x->headers = hs;
  x->nheaders = http2_stream_header_count(st);
  x->stages = k_stages;
  x->nstages = sizeof k_stages / sizeof k_stages[0];
  x->resp_limit = 1u << 20;
  x->max_decompressed = 64u << 20;
}

static http_exchange_t *stream_exchange(http2_stream_t *st) {
  stream_state_t *ss = http2_stream_ctx(st);
  if (ss == NULL) return NULL;
  if (ss->exchange == NULL) {
    ss->exchange = calloc(1, sizeof *ss->exchange);
    if (ss->exchange != NULL) {
      fill_exchange_meta(ss->exchange, st);
      ss->exchange->stages = k_stream_stages;
      ss->exchange->nstages = sizeof k_stream_stages / sizeof k_stream_stages[0];
      ss->exchange->body_limit = 65536;
    }
  }
  return ss->exchange;
}

static void submit_exchange(http2_session_t *s, http2_stream_t *st,
                            http_exchange_t *x);

static void handle_mw_stream_end(http2_session_t *s, http2_stream_t *st) {
  http_exchange_t *x = stream_exchange(st);
  if (x == NULL || x->response_sent) return;
  if (x->req_index == 0 && http_exchange_request(x) != HTTP_CONTINUE) {
    x->status = 400;
    x->resp_body = NULL;
    x->resp_body_len = 0;
    submit_exchange(s, st, x);
    return;
  }
  if (x->status == 0) x->status = 200;
  if (http_exchange_end(x) == HTTP_CONTINUE) {
    if (x->handler != NULL) x->handler(x);
    else if (x->req_body_len > 0) {
      x->resp_body = x->req_body;
      x->resp_body_len = x->req_body_len;
    }
  }
  submit_exchange(s, st, x);
}

static void submit_exchange(http2_session_t *s, http2_stream_t *st,
                            http_exchange_t *x) {
  if (x->response_sent) return;
  if (x->status == 0) x->status = 200;
  http_exchange_response(x);
  http_mw_writer_submit(s, st, x);
  x->response_sent = 1;
}

static void handle_mw_raw(http2_session_t *s, http2_stream_t *st,
                          const uint8_t *data, size_t len) {
  http_exchange_t *x = stream_exchange(st);
  if (x == NULL) return;
  if (x->response_sent) return;
  if (x->req_index == 0 && http_exchange_request(x) != HTTP_CONTINUE) {
    if (x->status == 0) x->status = 400;
    x->resp_body = NULL;
    x->resp_body_len = 0;
    submit_exchange(s, st, x);
    return;
  }
  int rc = http_exchange_data(x, data, len, 0);
  if (rc == HTTP_STOP) {
    if (x->status == 0) x->status = 400;
    x->resp_body = NULL;
    x->resp_body_len = 0;
    submit_exchange(s, st, x);
  } else if (rc != HTTP_CONTINUE) {
    http2_rst_stream(s, http2_stream_id(st));
  }
}

static http2_route_t k_routes[] = {
    {.path = "/3/7",
     .state_size = sizeof(stream_state_t),
     .state_free = stream_state_free,
     .stream_end = handle_mw_stream_end,
     .raw = handle_mw_raw},
    {.path = "/mw/echo",
     .state_size = sizeof(stream_state_t),
     .state_free = stream_state_free,
     .stream_end = handle_mw_stream_end,
     .raw = handle_mw_raw},
    {.path = "/mw/gzip",
     .state_size = sizeof(stream_state_t),
     .state_free = stream_state_free,
     .stream_end = handle_mw_stream_end,
     .raw = handle_mw_raw},
};

static const http2_route_t *resolve_route(void *ctx, const uint8_t *path,
                                           size_t path_len) {
  (void)ctx;
  for (size_t i = 0; i < sizeof k_routes / sizeof k_routes[0]; i++) {
    size_t n = strlen(k_routes[i].path);
    if (n == path_len && memcmp(k_routes[i].path, path, n) == 0)
      return &k_routes[i];
  }
  return NULL;
}

int main(void) {
  FixedString5 port = INIT_FSTR_LIT(FixedString5, "8000");
  signal(SIGPIPE, SIG_IGN); // a write to a fully-closed peer reports EPIPE,
                            // it must never kill the workers
  /* Optional log-level override (0=trace .. 5=fatal; set 6 for silence).
   * Unset keeps the library default, so behavior is unchanged. */
  const char *lvl = getenv("H2_LOG_LEVEL");
  if (lvl != NULL) {
    log_set_level(atoi(lvl));
  }
  /* Optional per-route receive-window override for the demo route; unset keeps
   * the session default chosen by H2_INITIAL_WINDOW_SIZE. */
  const char *rwin = getenv("H2_ROUTE_INITIAL_WINDOW");
  if (rwin != NULL && *rwin != '\0') {
    unsigned long long v = strtoull(rwin, NULL, 10);
    k_routes[0].initial_window =
        v > UINT32_MAX ? UINT32_MAX : (uint32_t)v;
  }
  http_mw_router_init(&k_router, k_mw_routes,
                      sizeof k_mw_routes / sizeof k_mw_routes[0]);
  http_mw_writer_init(&k_writer_mw, 1u << 20);
  http_mw_compression_init(&k_compression_mw);
  http_mw_body_limits_init(&k_body_limits_mw);
  http_mw_queue_limits_init(&k_queue_limits_mw);
  http_mw_body_init(&k_body_mw, 65536);
  k_stages[0] = *http_mw_logging_stage();
  k_stages[1] = *http_mw_router_stage(&k_router);
  k_stages[2] = *http_mw_queue_limits_stage(&k_queue_limits_mw);
  k_stages[3] = *http_mw_compression_stage(&k_compression_mw);
  k_stages[4] = *http_mw_body_limits_stage(&k_body_limits_mw);
  k_stages[5] = *http_mw_writer_stage(&k_writer_mw);
  k_stream_stages[0] = *http_mw_logging_stage();
  k_stream_stages[1] = *http_mw_router_stage(&k_router);
  k_stream_stages[2] = *http_mw_queue_limits_stage(&k_queue_limits_mw);
  k_stream_stages[3] = *http_mw_compression_stage(&k_compression_mw);
  k_stream_stages[4] = *http_mw_body_limits_stage(&k_body_limits_mw);
  k_stream_stages[5] = *http_mw_body_stage(&k_body_mw);
  k_stream_stages[6] = *http_mw_writer_stage(&k_writer_mw);
  http2_set_route_resolver(resolve_route, NULL);
  return Server_Init(&port) == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
