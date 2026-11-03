#include "http2_server.h"
#include "flow_control.h"
#include "liblog.h"
#include <errno.h>
#include <nghttp2/nghttp2.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>

/* All internal node structs fit this; larger/variable buffers use malloc. */
#define H2_NODE_SZ 224
#define H2_POOL_MAX 256
/* Small-object pools are bucketed into 16-byte size classes, so the pool also
 * serves the per-route stream state (whose size is arbitrary). Sizes above the
 * top class fall through to malloc/free. */
#define H2_CLASS_UNIT 16
#define H2_CLASS_MAX 16 /* pool sizes up to H2_CLASS_MAX * H2_CLASS_UNIT bytes */

typedef struct h2_out {
  uint8_t *payload;
  size_t len;
  size_t off;
  int eof;
  int charged;
  int owned;
  struct h2_out *next;
} h2_out_t;

typedef struct h2_flow {
  flow_policy_t policy;
  flow_window_t window;
} h2_flow_t;

struct http2_stream {
  int32_t id;
  http2_header_t *headers;
  http2_header_t *headers_tail;
  size_t header_count;
  http2_raw_fn raw;
  const http2_route_t *route;
  h2_out_t *out_head;
  h2_out_t *out_tail;
  int resp_opened;
  int resp_closed;
  int data_deferred;
  void *ctx;
  nghttp2_nv *trailer_nva;
  uint32_t trailer_nvlen;
  int trailers;
  h2_flow_t *flow;
  size_t queued_bytes;
};

_Static_assert(sizeof(http2_stream_t) <= H2_NODE_SZ, "stream fits pool");
_Static_assert(sizeof(http2_header_t) <= H2_NODE_SZ, "header fits pool");
_Static_assert(sizeof(h2_out_t) <= H2_NODE_SZ, "out node fits pool");
_Static_assert(H2_NODE_SZ <= H2_CLASS_MAX * H2_CLASS_UNIT, "node size pooled");

/* Per-class freelists: one worker thread owns its sessions, so a thread-local
 * pool needs no locks (alloc and free stay on one thread). */
static _Thread_local void *h2_pool[H2_CLASS_MAX];
static _Thread_local int h2_pool_n[H2_CLASS_MAX];

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

/* One parked in-flight NO_COPY frame per session. session_send() stops at
 * the first WOULDBLOCK, so at most one frame is ever mid-flight here.
 * flen tracks the CURRENT frame's payload length (frames chunk one node
 * across window updates); the node itself stays queued until fully framed. */
typedef struct {
  int active;
  int32_t sid;
  h2_out_t *msg;   /* borrowed while parked (still queued, not unlinked) */
  size_t flen;     /* this frame's payload length */
  size_t hdr_sent; /* frame-header bytes already on wire (0..9) */
  size_t sent;     /* this frame's payload bytes already on wire */
} h2_parked_t;

struct http2_session {
  nghttp2_session *session;
  int fd;
  pthread_t owner;         /* thread that created (and must drive) this session */
  http2_stream_t **slots; /* open-addressing id->stream index (power-of-two) */
  uint32_t slot_cap;      /* slot array length; 0 until the first stream */
  uint32_t slot_used;     /* live entries */
  uint32_t stream_window; /* per-stream local receive window target */
  uint64_t app_activity;  /* monotonic inbound request HEADERS/DATA count */
  int ping_ack_seen;
  uint64_t ping_ack_opaque;
  h2_parked_t parked;
  http2_write_fn write_fn;    /* NULL = direct write(fd, ...) */
  http2_writev_fn writev_fn;  /* NULL = direct writev(fd, ...) */
  void *transport_ctx;
  http2_flow_profile_t flow_profile;
  flow_policy_t flow_policy;
  flow_policy_t conn_flow_policy;
  flow_window_t conn_flow;
  uint32_t connection_window;
  int flow_enabled;
  size_t queued_bytes;
  size_t stream_queue_limit;
  size_t connection_queue_limit;
};

/* Sessions are worker-pinned; nghttp2 itself is not thread-safe. Enforce the
 * contract instead of letting a stray call corrupt the session silently. */
static void h2_require_owner(const http2_session_t *s) {
  if (s == NULL) {
    return;
  }
  if (!pthread_equal(s->owner, pthread_self())) {
    log_error("[h2] session API called from a non-owner thread");
    abort();
  }
}

static size_t h2_queue_limit(size_t value, size_t fallback) {
  return value != 0 ? value : fallback;
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

/* Internal forward declarations (these are no longer part of the public API). */
static http2_stream_t *http2_find_stream(http2_session_t *s, int32_t id);
static int http2_respond_headers(http2_session_t *s, int32_t sid,
                                 const nghttp2_nv *nva, size_t nvlen);
static int http2_send(http2_session_t *s, int32_t sid, uint8_t *payload,
                      size_t len, int end_stream);
static int http2_send_ex(http2_session_t *s, int32_t sid, uint8_t *payload,
                         size_t len, int end_stream, int owned);
static int h2_flow_profile_internal(http2_flow_profile_t profile);
static http2_route_resolver_fn g_route_resolver;
static void *g_route_ctx;

void http2_set_route_resolver(http2_route_resolver_fn fn, void *ctx) {
  g_route_resolver = fn;
  g_route_ctx = ctx;
}

static const http2_route_t *route_lookup(const uint8_t *path, size_t plen) {
  return g_route_resolver != NULL ? g_route_resolver(g_route_ctx, path, plen)
                                  : NULL;
}

static void wire_request_route(http2_session_t *s, http2_stream_t *st,
                               const http2_route_t *r) {
  if (r == NULL) {
    log_error("[h2] stream %d: unknown route", (int)st->id);
    http2_rst_stream(s, st->id);
    return;
  }
  if (r->raw == NULL) {
    log_error("[h2] stream %d: route has no raw handler", (int)st->id);
    http2_rst_stream(s, st->id);
    return;
  }
  st->raw = r->raw;
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

static void out_release(http2_session_t *s, http2_stream_t *st, h2_out_t *m) {
  if (m->charged) {
    if (st != NULL && st->queued_bytes >= m->len) {
      st->queued_bytes -= m->len;
    }
    if (s != NULL && s->queued_bytes >= m->len) {
      s->queued_bytes -= m->len;
    }
    m->charged = 0;
  }
  if (m->owned) free(m->payload);
  h2_node_free(m);
}

static void free_out_list(http2_session_t *s, http2_stream_t *st, h2_out_t *m) {
  while (m != NULL) {
    h2_out_t *next = m->next;
    out_release(s, st, m);
    m = next;
  }
}

/* --- response trailers ---------------------------------------------------- */

/* Deep-copy trailer fields: submission is deferred until the data provider
 * reports the last DATA byte, so the caller's nva may already be gone. */
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

/* Hand the stored trailers to nghttp2 (which copies them) and close the
 * response. The trailer block must follow DATA with END_STREAM clear, so
 * callers reach this only once the data provider is exhausted. */
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

/* --- O(1) id -> stream index: open addressing, linear probing --------------
 * Powers of two, load factor kept under 3/4. Deletion uses backward-shift so
 * there are no tombstones. A session is worker-pinned (single-threaded), so
 * no locks are needed. */
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

/* Discard one stream node and everything it owns (incl. its app ctx state). */
static void stream_free_node(http2_session_t *s, http2_stream_t *dead) {
  /* The parked in-flight frame (if any) is still linked in dead->out_head, so
   * clear the parking slot first; free_out_list() then releases it exactly
   * once. Clearing without freeing avoids a double-free. */
  if (s->parked.active && s->parked.msg != NULL && s->parked.sid == dead->id) {
    s->parked.active = 0;
    s->parked.msg = NULL;
  }
  free_headers(dead->headers);
  h2_trailers_free(dead);
  free_out_list(s, dead, dead->out_head);
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
  if (dead->flow != NULL) {
    h2_small_free(dead->flow, sizeof *dead->flow);
  }
  h2_node_free(dead);
}

static void free_stream(http2_session_t *s, int32_t id) {
  http2_stream_t *st = slot_find(s, id);
  if (st == NULL) {
    return;
  }
  slot_erase(s, id);
  stream_free_node(s, st);
}

static http2_stream_t *find_or_create_stream(http2_session_t *s, int32_t id) {
  http2_stream_t *st = slot_find(s, id);
  if (st != NULL) {
    return st;
  }
  st = h2_node_alloc();
  if (st == NULL) {
    return NULL;
  }
  st->id = id; /* zeroed until the route resolves at request HEADERS */
  if (slot_insert(s, st) != 0) {
    h2_node_free(st);
    return NULL;
  }
  return st;
}

/* nghttp2 pulls framed output bytes through here (small control frames).
 * Pushed straight to the (non-blocking) wire.
 *
 * NOTE: this nghttp2 build wants v2 (bytes-sent) semantics: return the
 * number of bytes written, NOT 0-on-success. Returning 0 means "sent
 * nothing, call me again" and resends the same frame forever. Only return
 * WOULDBLOCK when nothing could be written at all. */
static ssize_t on_send(nghttp2_session *session, const uint8_t *data,
                       size_t length, int flags, void *user_data) {
  (void)session;
  (void)flags;
  http2_session_t *s = user_data;
  size_t off = 0;
  for (;;) {
    if (off == length) {
      break;
    }
    ssize_t w = s->write_fn != NULL
                    ? s->write_fn(s->transport_ctx, data + off, length - off)
                    : write(s->fd, data + off, length - off);
    if (w > 0) {
      off += (size_t)w;
      continue;
    }
    if (w == 0) {
      break; /* cannot happen for length > 0; stop with progress so far */
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      break; /* return partial progress below (or WOULDBLOCK if none) */
    }
    log_error("[h2] send failed on fd %d: %s", s->fd, strerror(errno));
    return NGHTTP2_ERR_CALLBACK_FAILURE;
  }
  if (off == 0) {
    return NGHTTP2_ERR_WOULDBLOCK;
  }
  return (ssize_t)off;
}

/* Queue-peeking read side for the one-per-stream response provider.
 * Reports the head payload's length (NO_COPY: nothing is copied here);
 * the frame itself goes out through send_data_callback. Must stay
 * idempotent: retries after WOULDBLOCK re-invoke it with identical
 * results. An empty queue DEFERS (retried after resume_data on append). */
static ssize_t on_data_read(nghttp2_session *session, int32_t stream_id,
                            uint8_t *buf, size_t length, uint32_t *data_flags,
                            nghttp2_data_source *source, void *user_data) {
  (void)session;
  (void)buf;
  (void)length;
  http2_session_t *s = user_data;
  int32_t sid = (int32_t)(intptr_t)source->ptr;
  (void)stream_id;
  http2_stream_t *st = http2_find_stream(s, sid);
  if (st == NULL) {
    log_error("[h2] data_read: stream %d gone", (int)sid);
    return NGHTTP2_ERR_CALLBACK_FAILURE;
  }
  if (st->out_head == NULL) {
    st->data_deferred = 1;
    return NGHTTP2_ERR_DEFERRED;
  }
  /* Report only what fits this frame: the caller caps length at
   * min(windows, max_frame) via read_length (below). EOF goes on the
   * frame that carries the payload's final byte. */
  size_t rem = st->out_head->len - st->out_head->off;
  size_t n = rem < length ? rem : length;
  *data_flags |= NGHTTP2_DATA_FLAG_NO_COPY;
  if (st->out_head->next == NULL && n == rem &&
      (st->out_head->eof || st->trailers == 1)) {
    *data_flags |= NGHTTP2_DATA_FLAG_EOF;
    if (st->trailers == 1) {
      /* The trailer HEADERS carries END_STREAM, not this DATA frame. It is
       * legal to submit it here: nghttp2 emits it after the DATA frame. */
      *data_flags |= NGHTTP2_DATA_FLAG_NO_END_STREAM;
      if (h2_trailers_submit(s, st) != 0) {
        return NGHTTP2_ERR_CALLBACK_FAILURE;
      }
    }
  }
  return (ssize_t)n;
}

/* Caps each DATA frame at what the peer can actually take: min(session
 * window, stream window, max frame size, queued remainder). Without this,
 * a NO_COPY read reporting more than the window holds makes session_send
 * fail once cumulative sends pass 64KB. Floor of 1: nghttp2 only asks
 * when it intends to send, and anything <1 is a hard failure. */
static ssize_t on_read_length(nghttp2_session *session, uint8_t frame_type,
                              int32_t stream_id,
                              int32_t session_remote_window_size,
                              int32_t stream_remote_window_size,
                              uint32_t remote_max_frame_size, void *user_data) {
  (void)session;
  http2_session_t *s = user_data;
  size_t cap = remote_max_frame_size;
  if (frame_type != NGHTTP2_DATA) {
    return (ssize_t)cap;
  }
  http2_stream_t *st = http2_find_stream(s, stream_id);
  size_t rem = 0;
  if (st != NULL && st->out_head != NULL) {
    rem = st->out_head->len - st->out_head->off;
  }
  if (rem < cap) {
    cap = rem;
  }
  if (session_remote_window_size > 0 &&
      (size_t)session_remote_window_size < cap) {
    cap = (size_t)session_remote_window_size;
  }
  if (stream_remote_window_size > 0 &&
      (size_t)stream_remote_window_size < cap) {
    cap = (size_t)stream_remote_window_size;
  }
  if (cap < 1) {
    cap = 1;
  }
  return (ssize_t)cap;
}

static int on_send_data(nghttp2_session *session, nghttp2_frame *frame,
                        const uint8_t *framehd, size_t length,
                        nghttp2_data_source *source, void *user_data) {
  (void)session;
  (void)source; /* node identity comes from the stream head (see below) */
  http2_session_t *s = user_data;
  http2_stream_t *st = http2_find_stream(s, frame->hd.stream_id);
  if (st == NULL || st->out_head == NULL) {
    log_error("[h2] send_data: stream %d has nothing queued",
              (int)frame->hd.stream_id);
    return NGHTTP2_ERR_CALLBACK_FAILURE;
  }
  h2_out_t *m = st->out_head;
  h2_parked_t *p = &s->parked;

  if (!(p->active && p->msg == m)) {
    /* Fresh frame: park it (exactly one frame is ever mid-flight per
     * session -- session_send stops at the first WOULDBLOCK). flen is
     * this frame's share (see read_length); the node stays queued until
     * fully framed across one or more frames, then pops below. */
    p->active = 1;
    p->sid = frame->hd.stream_id;
    p->msg = m;
    p->flen = length;
    p->hdr_sent = 0;
    p->sent = 0;
  }
  if (frame->data.padlen != 0) {
    log_error("[h2] send_data: padded frames unsupported");
    return NGHTTP2_ERR_CALLBACK_FAILURE;
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
      break; /* fully accounted (covers zero-length payloads) */
    }
    ssize_t w = s->writev_fn != NULL
                    ? s->writev_fn(s->transport_ctx, iov, n)
                    : writev(s->fd, iov, n);
    if (w < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return NGHTTP2_ERR_WOULDBLOCK; /* parked offsets kept; retried */
      }
      log_error("[h2] send_data failed on fd %d: %s", s->fd, strerror(errno));
      return NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    if (w == 0) {
      return NGHTTP2_ERR_WOULDBLOCK; /* no progress; retry on writable */
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
        st->out_head = m->next; /* pop the fully framed head */
        if (st->out_head == NULL) {
          st->out_tail = NULL;
        }
        if (was_eof) {
          st->resp_closed = 1;
        }
        out_release(s, st, m);
      }
      p->active = 0;
      p->msg = NULL;
      return 0;
    }
  }
  /* Fell through only when everything was already accounted for. */
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
    out_release(s, st, m);
  }
  p->active = 0;
  p->msg = NULL;
  return 0;
}

/* Store every header field (incl. pseudo-headers and trailers) on the
 * stream, in arrival order. */
static int on_header(nghttp2_session *session, const nghttp2_frame *frame,
                     const uint8_t *name, size_t namelen, const uint8_t *value,
                     size_t valuelen, uint8_t flags, void *user_data) {
  (void)session;
  (void)flags;
  if (frame->hd.type != NGHTTP2_HEADERS) {
    return 0;
  }
  http2_session_t *s = user_data;
  http2_stream_t *st = find_or_create_stream(s, frame->hd.stream_id);
  if (st == NULL) {
    return NGHTTP2_ERR_CALLBACK_FAILURE;
  }
  if (st->header_count >= HTTP2_MAX_HEADERS) {
    log_error("[h2] stream %d: too many headers", (int)st->id);
    return NGHTTP2_ERR_CALLBACK_FAILURE;
  }
  http2_header_t *h = h2_node_alloc();
  if (h == NULL) {
    return NGHTTP2_ERR_CALLBACK_FAILURE;
  }
  /* One allocation holds the name bytes then the value bytes; h->value
   * aliases into the same block, so free_headers() releases just h->name. */
  size_t blen = namelen + valuelen;
  uint8_t *blk = malloc(blen > 0 ? blen : 1);
  if (blk == NULL) {
    h2_node_free(h);
    return NGHTTP2_ERR_CALLBACK_FAILURE;
  }
  memcpy(blk, name, namelen);
  memcpy(blk + namelen, value, valuelen);
  h->name = blk;
  h->value = blk + namelen;
  h->namelen = namelen;
  h->valuelen = valuelen;
  /* Append to keep arrival order. */
  if (st->headers_tail != NULL) {
    st->headers_tail->next = h;
  } else {
    st->headers = h;
  }
  st->headers_tail = h;
  st->header_count++;
  /* Resolve routing while the header block is already being walked, so
   * on_frame_recv does not need to rescan the list for :path. */
  if (frame->headers.cat == NGHTTP2_HCAT_REQUEST && st->route == NULL &&
      g_route_resolver != NULL && namelen == 5 && memcmp(name, ":path", 5) == 0) {
    st->route = route_lookup(value, valuelen);
  }
  return 0;
}

void http2_rst_stream(http2_session_t *s, int32_t sid) {
  if (s == NULL) {
    return;
  }
  h2_require_owner(s);
  nghttp2_submit_rst_stream(s->session, 0, sid, NGHTTP2_PROTOCOL_ERROR);
}


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

/* Return `n` received DATA bytes to the flow-control window. The bytes were
 * counted against both the stream and the connection window, so credit the
 * connection even when the stream is already gone -- otherwise a reset stream
 * would leak connection credit and eventually stall every other stream. */
static int h2_consume_stream_now(http2_session_t *s, int32_t sid, size_t n) {
  if (n == 0) {
    return 0;
  }
  if (!s->flow_enabled) {
    if (slot_find(s, sid) != NULL &&
        nghttp2_session_consume(s->session, sid, n) == 0) {
      return 0;
    }
    return nghttp2_session_consume_connection(s->session, n) == 0 ? 0 : -1;
  }

  http2_stream_t *st = slot_find(s, sid);
  uint64_t now = h2_now_ms();
  flow_credit_t stream_credit = {FLOW_CREDIT_NONE, 0, 0};
  flow_credit_t conn_credit = {FLOW_CREDIT_NONE, 0, 0};

  if (st != NULL && st->flow != NULL) {
    flow_window_on_consumed(&st->flow->window, (uint32_t)n);
    stream_credit = flow_window_update(&st->flow->window, now);
  }
  flow_window_on_consumed(&s->conn_flow, (uint32_t)n);
  conn_credit = flow_window_update(&s->conn_flow, now);

  int rc = 0;
  if (st != NULL && st->flow != NULL) {
    rc |= h2_apply_stream_credit(s, st, stream_credit);
  }
  rc |= h2_apply_connection_credit(s, conn_credit);
  return rc != 0 ? -1 : 0;
}

static int h2_consume_stream(http2_session_t *s, int32_t sid, size_t n) {
  return h2_consume_stream_now(s, sid, n);
}

static int on_data_chunk(nghttp2_session *session, uint8_t flags,
                         int32_t stream_id, const uint8_t *data, size_t len,
                         void *user_data) {
  (void)session;
  (void)flags;
  http2_session_t *s = user_data;
  s->app_activity++;
  http2_stream_t *st = slot_find(s, stream_id);
  if (st == NULL) {
    log_error("[h2] DATA for unknown stream %d", (int)stream_id);
    return NGHTTP2_ERR_CALLBACK_FAILURE;
  }
  h2_flow_received(s, st, len);
  if (st->raw == NULL) {
    h2_consume_stream_now(s, st->id, len);
    return 0;
  }
  st->raw(s, st, data, len);
  h2_consume_stream_now(s, st->id, len);
  return 0;
}

static int on_frame_recv(nghttp2_session *session, const nghttp2_frame *frame,
                         void *user_data) {
  (void)session;
  http2_session_t *s = user_data;
  if (frame->hd.type == NGHTTP2_PING &&
      (frame->hd.flags & NGHTTP2_FLAG_ACK)) {
    memcpy(&s->ping_ack_opaque, frame->ping.opaque_data,
           sizeof s->ping_ack_opaque);
    s->ping_ack_seen = 1;
  }
  /* Request HEADERS are application activity; control frames are not. */
  if (frame->hd.stream_id != 0 && frame->hd.type == NGHTTP2_HEADERS) {
    s->app_activity++;
  }
  /* Resolve a stream once per HEADERS/DATA frame: routing and end-of-stream
   * finalization both need it. */
  http2_stream_t *st = NULL;
  if (frame->hd.stream_id != 0 &&
      (frame->hd.type == NGHTTP2_HEADERS ||
       frame->hd.type == NGHTTP2_DATA)) {
    st = slot_find(s, frame->hd.stream_id);
  }
  /* At request HEADERS, before any DATA: raise this stream's local receive
   * window. The route may override the session default. Routing is resolved
   * first so the response can still be emitted immediately for a bodyless
   * END_STREAM. The WINDOW_UPDATE is what grants the peer the same credit (the
   * initial window is not advertised in SETTINGS). */
  if (frame->hd.stream_id != 0 && frame->hd.type == NGHTTP2_HEADERS &&
      frame->headers.cat == NGHTTP2_HCAT_REQUEST) {
    const http2_route_t *r = st != NULL ? st->route : NULL;
    uint32_t win = s->stream_window;
    if (r != NULL && r->initial_window != 0) {
      win = r->initial_window;
    }
    if (win > (uint32_t)NGHTTP2_MAX_WINDOW_SIZE) {
      win = (uint32_t)NGHTTP2_MAX_WINDOW_SIZE;
    }
    if (win > s->connection_window) {
      win = s->connection_window;
    }
    if (st != NULL &&
        h2_stream_flow_init(s, st, win,
                            r != NULL && r->flow_profile != HTTP2_FLOW_INHERIT
                                ? r->flow_profile
                                : s->flow_profile) != 0) {
      log_error("[h2] stream %d: flow-control initialization failed",
                (int)frame->hd.stream_id);
      return NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    if (nghttp2_session_set_local_window_size(
            s->session, NGHTTP2_FLAG_NONE, frame->hd.stream_id,
            (int32_t)win) != 0) {
      log_error("[h2] stream %d: set_local_window_size failed",
                (int)frame->hd.stream_id);
    }
    if (st != NULL && st->route != NULL) {
      wire_request_route(s, st, r);
    }
  }
  /* Finalize inbound assembly at end of request. */
  if ((frame->hd.type == NGHTTP2_HEADERS || frame->hd.type == NGHTTP2_DATA) &&
      (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) &&
      frame->hd.stream_id != 0) {
    if (st == NULL) {
      return 0;
    }
    if (st->raw != NULL && st->route != NULL &&
        st->route->stream_end != NULL) {
      st->route->stream_end(s, st);
    }
  }
  return 0;
}

static int on_stream_close(nghttp2_session *session, int32_t stream_id,
                           uint32_t error_code, void *user_data) {
  (void)session;
  (void)error_code;
  free_stream(user_data, stream_id);
  return 0;
}

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
  if (stream_window_size > connection_window_size) {
    return NULL;
  }

  http2_session_t *s = calloc(1, sizeof(*s));
  if (s == NULL) {
    return NULL;
  }
  s->fd = client_fd;
  s->owner = pthread_self();
  s->stream_window = stream_window_size;
  s->connection_window = connection_window_size;
  s->flow_profile = HTTP2_FLOW_DEFAULT;
  flow_policy_profile(FLOW_PROFILE_DEFAULT, stream_window_size,
                      connection_window_size, &s->flow_policy);
  flow_policy_profile(FLOW_PROFILE_DEFAULT, connection_window_size,
                      connection_window_size, &s->conn_flow_policy);
  uint32_t flow_warnings = 0;
  if (flow_policy_validate(&s->flow_policy, &flow_warnings) != FLOW_OK ||
      flow_policy_validate(&s->conn_flow_policy, &flow_warnings) != FLOW_OK ||
      flow_window_init(&s->conn_flow, &s->conn_flow_policy,
                       connection_window_size) != 0) {
    free(s);
    return NULL;
  }
  s->flow_enabled = 1;
  s->stream_queue_limit = HTTP2_DEFAULT_STREAM_QUEUE;
  s->connection_queue_limit = HTTP2_DEFAULT_CONNECTION_QUEUE;
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

  /* Manual flow-control accounting: the module credits bytes as it delivers
   * them. The application controls credit timing through consume calls. */
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


int http2_session_feed(http2_session_t *s, const uint8_t *data, size_t len) {
  if (s == NULL) {
    return -1;
  }
  h2_require_owner(s);
  nghttp2_ssize consumed = nghttp2_session_mem_recv2(s->session, data, len);
  if (consumed < 0) {
    log_error("[h2] mem_recv: %s", nghttp2_strerror((int)consumed));
    return -1;
  }
  /* Flush anything queued (responses, RST_STREAM, ...), but only if nghttp2
   * actually has bytes pending -- avoids a redundant session_send() call on
   * the common inbound-only path (pure ACKs/settings are rare). */
  if (nghttp2_session_want_write(s->session)) {
    int srv = nghttp2_session_send(s->session);
    if (srv != 0 && srv != NGHTTP2_ERR_WOULDBLOCK) {
      log_error("[h2] session_send failed: %s (%d)", nghttp2_strerror(srv),
                srv);
      return -1;
    }
  }
  return 0;
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
    out_release(s, NULL, s->parked.msg);
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

uint64_t http2_session_app_activity(const http2_session_t *s) {
  if (s == NULL) {
    return 0;
  }
  h2_require_owner(s);
  return s->app_activity;
}


int32_t http2_stream_id(const http2_stream_t *st) {
  return st != NULL ? st->id : 0;
}

void *http2_stream_ctx(const http2_stream_t *st) {
  return st != NULL ? st->ctx : NULL;
}

const http2_header_t *http2_stream_headers(const http2_stream_t *st) {
  return st != NULL ? st->headers : NULL;
}
size_t http2_stream_header_count(const http2_stream_t *st) {
  return st != NULL ? st->header_count : 0;
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
  if (!h2_queue_fits(s, st, len)) {
    log_error("[h2] respond_msg: stream %d queue limit exceeded", (int)sid);
    http2_rst_stream(s, sid);
    return -1;
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

int http2_respond_msg_ref(http2_session_t *s, int32_t sid, const uint8_t *body,
                          size_t len, int end_stream) {
  if (s == NULL || sid <= 0 || (len > 0 && body == NULL)) return -1;
  h2_require_owner(s);
  http2_stream_t *st = http2_find_stream(s, sid);
  if (st == NULL || st->resp_closed || st->trailers != 0) return -1;
  if (len == 0) return http2_respond_msg(s, sid, NULL, 0, end_stream);
  if (!st->resp_opened && http2_respond_begin(s, sid, 200, NULL, 0) != 0)
    return -1;
  if (http2_send_ex(s, sid, (uint8_t *)body, len, end_stream, 0) != 0) {
    http2_rst_stream(s, sid);
    return -1;
  }
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

static int http2_send_ex(http2_session_t *s, int32_t sid, uint8_t *payload,
                         size_t len, int end_stream, int owned) {
  if (s == NULL || sid <= 0 || (len > 0 && payload == NULL)) {
    if (owned) free(payload);
    return -1;
  }
  http2_stream_t *st = http2_find_stream(s, sid);
  if (st == NULL) {
    if (owned) free(payload);
    return -1;
  }
  if (st->resp_closed || st->trailers != 0) {
    log_error("[h2] send after EOF on stream %d", (int)sid);
    if (owned) free(payload);
    return -1;
  }
  if (!h2_queue_fits(s, st, len)) {
    log_error("[h2] stream %d: response queue limit exceeded", (int)sid);
    free(payload);
    return -1;
  }
  /* One node per payload, any size: the read_length callback below chunks
   * it into window-sized frames at send time (nghttp2 will not fragment
   * NO_COPY payloads itself). Ownership transfers to the queue. */
  h2_out_t *m = h2_node_alloc();
  if (m == NULL) {
    if (owned) free(payload);
    return -1;
  }
  m->payload = payload;
  m->len = len;
  m->eof = end_stream ? 1 : 0;
  m->charged = 1;
  m->owned = owned;
  m->next = NULL;
  st->queued_bytes += len;
  s->queued_bytes += len;
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

static int http2_send(http2_session_t *s, int32_t sid, uint8_t *payload,
                      size_t len, int end_stream) {
  return http2_send_ex(s, sid, payload, len, end_stream, 1);
}


nghttp2_session *http2_raw(http2_session_t *s) {
  if (s == NULL) {
    return NULL;
  }
  h2_require_owner(s);
  return s->session;
}

int http2_flush(http2_session_t *s) {
  if (s == NULL) {
    return -1;
  }
  h2_require_owner(s);
  /* Skip the session_send() call entirely when nothing is queued. */
  if (!nghttp2_session_want_write(s->session)) {
    return 0;
  }
  int srv = nghttp2_session_send(s->session);
  if (srv != 0 && srv != NGHTTP2_ERR_WOULDBLOCK) {
    log_error("[h2] flush failed: %s (%d)", nghttp2_strerror(srv), srv);
    return -1;
  }
  return 0;
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

