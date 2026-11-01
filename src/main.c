/*
 ============================================================================
  Name        : tcp_server_hacking
  Version     : 1.0
  Description : A simple C application demonstrating project structure with
                src, include, Makefile-based build system, and modular design.
                Uses static libraries and includes dependency management.

  The file is laid out exactly like the HTTP/2 app boilerplate:
    [ FRAMEWORK ]  the framing rule and the per-stream state layout.
    [ WIRE SCHEMA ] your message shape (a list of fixed fields and u16
                    length-prefixed blobs).
    [ YOUR CODE ]  this project's handler and route.

  Inbound assembly (residual buffering, seam compaction, one-time params,
  END_STREAM validation), :path route resolution, dispatch, and per-stream
  state allocation all live in the http2 module now; this file only supplies the
  wire schema (msg_framing), the routes, and the handlers.
 ============================================================================
*/

#include "../include/custring.h"
#include "../include/http2.h"
#include "../include/liblog.h"
#include "../include/server.h"
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
 * [ WIRE SCHEMA ] your message shape: a `layout` list of fixed-size scalars and
 * u16-BE length-prefixed blobs, in wire order. Each blob's prefix holds the byte
 * length of that field (an array is one blob; the handler decodes inside it).
 *
 * This type is the opaque `framing_cfg` you set on each route; the module hands
 * it back to msg_framing() below.
 * ========================================================================= */
typedef struct {
  uint8_t blob;  /* 0 = fixed `size` bytes; 1 = u16-BE length-prefixed blob */
  uint16_t size; /* byte count, when blob == 0 */
} wire_field_t;

typedef struct {
  const wire_field_t *layout; /* fields in wire order */
  size_t layout_n;            /* number of fields */
} schema_t;

/* ============================================================================
 * [ FRAMEWORK ] per-stream state + HTTP/2 plumbing.
 * ========================================================================= */

/* Per-stream app state, hung off http2_stream_t.ctx. Each route declares its
 * own type via http2_route_t.state_size; the module allocates it zeroed at the
 * request HEADERS and frees it (after any state_free) at stream close, so it
 * survives stream close and session teardown alike. Handlers cast the context back
 * to this type. The module owns the byte residual and the one-time params now
 * (read them with http2_prefix); keep here only what your handlers need. */
typedef struct stream_state {
  const http2_route_t *route;
  int32_t sid;
} stream_state_t;

/* ============================================================================
 * [ WIRE SCHEMA ] the framing rule. The module calls this with a route's
 * `framing_cfg` (a schema_t): fixed fields advance a running total; each u16-BE
 * blob contributes its 2-byte prefix plus that many bytes. Returns the length of
 * the message at the cursor, or 0 while still incomplete. Replace it if your
 * wire format differs; the module consumes only this. Unary validation rides on
 * it too -- the module requires the whole body to be exactly one such message.
 * ========================================================================= */

/* Length of the next complete message at the cursor, or 0. This is the
 * module's http2_msg_framing_cb; `cfg` is the route's schema_t. */
static size_t msg_framing(const void *cfg, const http2_view_t *v, size_t cur,
                          size_t avail) {
  const schema_t *r = cfg;
  size_t total = 0;
  for (size_t i = 0; i < r->layout_n; i++) {
    const wire_field_t *f = &r->layout[i];
    if (!f->blob) {
      total += f->size;
      continue;
    }
    if (avail < total + 2) {
      return 0; /* u16 prefix not fully visible yet */
    }
    uint8_t t0 = http2_view_byte(v, cur + total);
    uint8_t t1 = http2_view_byte(v, cur + total + 1);
    total += 2 + (size_t)(((uint16_t)t0 << 8) | t1);
  }
  return avail >= total ? total : 0;
}

/* ============================================================================
 * ==============================  YOUR CODE  =================================
 * This project's handler and route. Wire integers are fixed-width big-endian;
 * strings are Pascal-style (u16-BE length + raw bytes).
 * ========================================================================= */

/* svc=0x03 method=0x07 Session.GetStats (example object):
 *   u32 status | u16 active | u16 idle | u8 flags | u16 name_len | name[]
 * fixed = 11; the Pascal name tail starts at offset 9. */
static void handle_getstats(http2_session_t *s, http2_stream_t *st,
                            const uint8_t *p, size_t len) {
  (void)len; /* the module guarantees 11B fixed + a full Pascal tail */
  uint32_t status = rd32(p + 0);
  uint16_t active = rd16(p + 4);
  uint16_t idle = rd16(p + 6);
  uint8_t flags = p[8];
  uint16_t nlen = rd16(p + 9);
  const uint8_t *name = p + 11;
  log_info("getstats status=%u active=%u idle=%u flags=%u name=%.*s", status,
           active, idle, flags, (int)nlen, (const char *)name);
  http2_respond_msg(s, http2_stream_id(st), name, nlen, 1); /* return the name */
}

/* Session.GetStats under the :path key "/3/7", unary, shipped wire schema. */
static const wire_field_t layout_3_7[] = {
    {.blob = 0, .size = 9}, /* status(4) active(2) idle(2) flags(1) */
    {.blob = 1},            /* u16 name_len | name */
};

static const schema_t schema_3_7 = {
    layout_3_7,
    sizeof layout_3_7 / sizeof layout_3_7[0],
};

static http2_route_t k_routes[] = {
    {.path = "/3/7",
     .mode = HTTP2_UNARY,
     .params_len = 0,
     .msg_framing = msg_framing,
     .framing_cfg = &schema_3_7,
     .state_size = sizeof(stream_state_t),
     .handler.unary = handle_getstats},
};

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
  for (size_t i = 0; i < sizeof k_routes / sizeof k_routes[0]; i++) {
    if (http2_route_add(&k_routes[i]) != 0) {
      log_error("app: route table full");
      return EXIT_FAILURE;
    }
  }
  return Server_Init(&port) == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
