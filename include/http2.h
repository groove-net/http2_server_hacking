#ifndef HTTP2_H
#define HTTP2_H

/* Neutral HTTP/2 server layer (h2c, prior knowledge) on nghttp2.
 *
 * nghttp2 does framing + HPACK only, no I/O: the engine reads bytes off the
 * wire and pushes them here via http2_session_feed(); framed output bytes
 * go back out through the session's send callbacks.
 *
 * Zero-copy design (both directions):
 * - RECEIVE: the module owns inbound assembly. A streaming stream is framed
 *   into whole messages out of the DATA flow and handed to the handler a BATCH
 *   at a time -- each message pointed straight into chunk memory when it lies
 *   wholly inside one segment, compacted once only when it straddles the
 *   residual/chunk seam. A unary stream buffers the whole body and hands it over
 *   once at END_STREAM. Either way the module stores the one-time params region
 *   (http2_prefix). nghttp2 itself still copies inbound bytes once internally --
 *   irreducible via its public API.
 * - SEND: a response is queued through the respond family (http2_respond_msg
 *   / http2_respond_msg_owned / http2_respond_end / http2_respond_trailers);
 *   the module owns the payload
 *   and emits it via NGHTTP2_DATA_FLAG_NO_COPY + writev: zero userspace copies
 *   from its buffer to the kernel. http2_respond_msg() copies a borrowed body
 *   in (unavoidable, the caller does not own it); http2_respond_msg_owned()
 *   instead takes ownership of a malloc-compatible buffer, so an
 *   already-owned payload reaches the kernel with no userspace copy at all.
 *   Freed exactly once: on full send, or on stream reset/close.
 *
 * Ownership/lifetime:
 * - Streams, headers and accumulated bodies are BORROWED handles, freed on
 *   stream close; copy anything you need to keep past that point.
 * - One http2_session_t lives per connection, driven by exactly one worker
 *   thread (nghttp2 sessions are not thread-safe). Hooks run on that
 *   thread and must be thread-safe if they touch shared state.
 * - Session APIs enforce that ownership. Calling one from another thread is a
 *   programming error and aborts. The only exceptions are construction and the
 *   process-wide budget setter.
 */

#include <nghttp2/nghttp2.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

struct iovec;

/* Caps against unbounded peer-driven growth. Tweak as needed. */
enum {
  HTTP2_DEFAULT_WINDOW_SIZE = 65535,        /* HTTP/2 initial receive window */
  HTTP2_MAX_HEADERS = 128,                  /* header fields stored per stream */
  HTTP2_MAX_BODY = 4u * 1024u * 1024u,     /* unary body buffered per stream */
  HTTP2_MAX_RESIDUAL = 1u * 1024u * 1024u, /* streaming: incomplete-tail cap */
  HTTP2_BATCH_MAX = 64, /* streaming: messages handed to the handler per call */
};

/* Borrowed two-segment view over the inbound flow: segment `a` is the residual
 * left from earlier chunks, `b` the freshly-arrived chunk. Framing reads across
 * the seam without copying. `an` may be 0. */
typedef struct http2_view {
  const uint8_t *a;
  size_t an;
  const uint8_t *b;
  size_t bn;
} http2_view_t;

/* i-th pending byte; caller guarantees i < an + bn. */
static inline uint8_t http2_view_byte(const http2_view_t *v, size_t i) {
  return i < v->an ? v->a[i] : v->b[i - v->an];
}

/* Framing rule: total byte length of the message beginning at view offset
 * `cur`, or 0 if not yet determinable (need more bytes). May read only within
 * [cur, cur+avail). Must be pure: same inputs => same answer. `cfg` is the
 * opaque config you set on the route (see http2_route_t.framing_cfg). This
 * decouples the module from your wire format: length-prefixed, delimited,
 * sentinel, or scanned (JSON) all reduce to "how long is the next message",
 * provided the rule is a pure function of the bytes seen so far. A streaming
 * route MUST supply one; a unary route may omit it (the whole body is then
 * handed to the handler unvalidated). */
typedef size_t (*http2_msg_framing_cb)(const void *cfg, const http2_view_t *v,
                                       size_t cur, size_t avail);

struct http2_session;
struct http2_stream;
typedef struct http2_session http2_session_t;
typedef struct http2_stream http2_stream_t;

/* One framed message in a streaming batch. Borrowed for the handler call only
 * and may point into chunk memory; copy anything you retain. */
typedef struct http2_msg {
  const uint8_t *msg;
  size_t len;
} http2_msg_t;

/* A bundle of whole messages handed to a streaming handler. The SAME type is
 * used whether the stream is attached (module-owned, valid only for the call) or
 * detached (you own it, keep it as long as you like). Read the messages with
 * http2_batch_msgs() and finish with http2_batch_release() either way; release is
 * a no-op for an attached batch and frees a detached one. Call release exactly
 * once, when you are done (immediately, or later if deferring). */
typedef struct http2_batch http2_batch_t;

/* Messages in the batch and their count. Borrowed for the batch's lifetime. */
const http2_msg_t *http2_batch_msgs(const http2_batch_t *b, size_t *n);

/* 1 if this batch is yours to free (detached), 0 if module-owned (attached). */
int http2_batch_owned(const http2_batch_t *b);

/* Release the batch: free it if detached, no-op if attached. Idempotent-safe
 * only for attached; a detached batch must be released exactly once. */
void http2_batch_release(http2_batch_t *b);

/* Receive-side backpressure. The module turns off nghttp2's automatic
 * WINDOW_UPDATE and credits bytes itself as it buffers or synchronously
 * delivers them. For a DETACHED batch the bytes stay uncredited until the
 * consumer reports progress: after processing the batch, call
 * http2_batch_consume() (or http2_stream_consume()) before releasing it.
 * While any detached batch is uncredited the peer eventually stops sending
 * DATA, which is the intended backpressure. These calls must run on the
 * session's owning thread (sessions are not thread-safe). */

/* Wire bytes in this batch that still need credit; 0 for attached batches
 * (already credited when the handler returned). */
size_t http2_batch_consume_bytes(const http2_batch_t *b);

/* Credit `n` received DATA bytes back to the stream/connection windows and
 * flush the resulting WINDOW_UPDATE. Returns 0/-1. */
int http2_stream_consume(struct http2_session *s, int32_t sid, size_t n);

/* Credit a detached batch's owed bytes (idempotent) and flush. Call before
 * http2_batch_release(). Safe on attached batches (no-op). Returns 0/-1. */
int http2_batch_consume(struct http2_session *s, http2_batch_t *b);

/* Stream mode, fixed for a stream's life. */
typedef enum { HTTP2_UNARY = 0, HTTP2_STREAMING = 1 } http2_mode_t;

/* Streaming sink: called per BATCH of whole messages framed out of the current
 * chunk, at most HTTP2_BATCH_MAX per call. Read them from the batch and call
 * http2_batch_release() when done. For an attached stream the batch is valid
 * only for the call (zero-copy); for a detached stream you own it and may
 * release it later. One handler serves both. */
typedef void (*http2_stream_fn)(struct http2_session *s,
                                struct http2_stream *st, http2_batch_t *batch);

/* Unary sink: called EXACTLY once with the whole (validated) body. */
typedef void (*http2_unary_fn)(struct http2_session *s,
                               struct http2_stream *st, const uint8_t *body,
                               size_t len);

/* The route registry. Register routes once before serving; the module resolves
 * each request's :path itself, binds the matched route's framing+handler to the
 * stream, and resets unknown routes. `framing_cfg` is opaque and handed back to
 * msg_framing(). Setting `detach` on a streaming route makes the module hand
 * each batch to the handler as an OWNED bundle (see http2_batch_t) instead of a
 * borrowed one -- the only difference a handler sees is when it may release.
 *
 * Per-route stream state: set `state_size` to the size of your route's state
 * struct. The module then allocates zeroed per-stream state (`stream->ctx`,
 * sizeof == state_size) the moment the route resolves at the request HEADERS,
 * runs `state_init(ctx)` if set, casts nothing (the struct is yours), and on
 * stream close runs `state_free(ctx)` if set before freeing it. Each route may
 * therefore use a DIFFERENT struct type; handlers recover it with a plain cast
 * of stream->ctx. Tip: give every struct a shared `base` prefix (route, sid,
 * producer handles, closed flag) so generic close/cancel code can use the base
 * without knowing the concrete type. Leave `state_size` 0 for no per-stream
 * state (stream->ctx stays NULL). */
/* Flow-control profile selected for a session or route. INHERIT is only valid
 * on a route and means "use the session profile". */
typedef enum {
  HTTP2_FLOW_INHERIT = 0,
  HTTP2_FLOW_DEFAULT,
  HTTP2_FLOW_LOW_LATENCY,
  HTTP2_FLOW_BULK,
} http2_flow_profile_t;

typedef struct http2_route {
  const char *path;                 /* opaque :path key, matched byte-for-byte */
  http2_mode_t mode;                /* HTTP2_UNARY or HTTP2_STREAMING */
  int detach;                       /* streaming: hand owned batches (see below) */
  size_t params_len;                /* streaming: one-time params bytes */
  http2_msg_framing_cb msg_framing; /* unary: optional validator; streaming:
                                        required message framer */
  const void *framing_cfg;          /* opaque; passed back to msg_framing() */
  size_t state_size;                /* bytes of per-stream ctx (0 = app-managed) */
  void (*state_init)(void *ctx);    /* optional ctor, run once at HEADERS */
  void (*state_free)(void *ctx);    /* optional dtor, run at stream close */
  union {
    http2_unary_fn unary;           /* handler when mode == HTTP2_UNARY */
    http2_stream_fn stream;         /* handler when mode == HTTP2_STREAMING */
  } handler;
  /* Optional per-route stream receive window, applied with a WINDOW_UPDATE at
   * request HEADERS. 0 keeps the session default from http2_session_new(). */
  uint32_t initial_window;
  http2_flow_profile_t flow_profile; /* INHERIT = session profile */
} http2_route_t;

/* Register a route. `r` is copied; `r->path` and `r->framing_cfg` must outlive
 * the process. Returns 0, or -1 if the table is full. Call before serving. */
int http2_route_add(const http2_route_t *r);

/* One raw header field, in arrival order. Names/values are NOT
 * NUL-terminated (HPACK binary) -- always use namelen/valuelen.
 * Pseudo-headers (":method", ":path", ...) appear here like any other. */
typedef struct http2_header {
  uint8_t *name;
  size_t namelen;
  uint8_t *value;
  size_t valuelen;
  struct http2_header *next;
} http2_header_t;

/* Stream ID and application context. The stream representation is private. */
int32_t http2_stream_id(const http2_stream_t *st);
void *http2_stream_ctx(const http2_stream_t *st);
const http2_header_t *http2_stream_headers(const http2_stream_t *st);


/* Optional write transport. The callbacks must not block. Return the number
 * of bytes accepted, 0 if the transport cannot accept more now, or a negative
 * value on fatal error. `writev` receives the same scatter/gather buffers the
 * plaintext path would pass to writev(). A transport is copied into the
 * session; its context must outlive the session.
 */
typedef ssize_t (*http2_write_fn)(void *ctx, const uint8_t *buf, size_t len);
typedef ssize_t (*http2_writev_fn)(void *ctx, const struct iovec *iov,
                                  int iovcnt);

typedef struct http2_transport {
  void *ctx;
  http2_write_fn write;
  http2_writev_fn writev;
} http2_transport_t;

/* Create a server session bound to `client_fd` (must be non-blocking).
 * `stream_window_size` raises each stream's local receive window at request
 * HEADERS (one WINDOW_UPDATE per stream); `connection_window_size` raises the
 * connection-level receive window (one WINDOW_UPDATE per connection). Both are
 * in bytes and must be nonzero; use HTTP2_DEFAULT_WINDOW_SIZE for the standard
 * 65535-byte window. Values above NGHTTP2_MAX_WINDOW_SIZE are clamped, but
 * zero is rejected.
 *
 * The stream window is applied with WINDOW_UPDATE rather than advertised via
 * SETTINGS_INITIAL_WINDOW_SIZE: nghttp2 does not mirror that setting into its
 * local window, so advertising it here as well would double the peer's send
 * window relative to what the module enforces. Larger windows raise throughput
 * on high-BDP links; the one-per-stream WINDOW_UPDATE is why the server layer
 * passes 0 by default. The module's per-stream body/residual caps still bound
 * memory. Individual routes may override the stream window with
 * http2_route_t.initial_window.
 *
 * Emits the server connection preface (SETTINGS). Returns NULL on failure.
 */
http2_session_t *http2_session_new(int client_fd,
                                   uint32_t stream_window_size,
                                   uint32_t connection_window_size);

/* Select the flow-control profile for streams created after this call. The
 * profile is validated internally; the default is HTTP2_FLOW_DEFAULT. Existing
 * streams keep the profile they were created with. Returns 0, or -1 for an
 * invalid profile or session. */
int http2_session_set_flow_profile(http2_session_t *s,
                                   http2_flow_profile_t profile);

/* Return the session's current default flow-control profile. */
http2_flow_profile_t http2_session_flow_profile(const http2_session_t *s);

/* General-purpose transport variant. The response API is unchanged: responses
 * are written through `transport` instead of directly to `client_fd`. Pass a
 * TLS writer for encrypted output, or use http2_session_new() for the original
 * plaintext direct-fd path. Returns NULL for an invalid transport.
 */
http2_session_t *http2_session_new_transport(const http2_transport_t *transport,
                                             int client_fd,
                                             uint32_t stream_window_size,
                                             uint32_t connection_window_size);

/* Process-wide cap on unary request-body buffers held across all sessions.
 * 0 = unlimited (the default). When a new chunk would exceed it the offending
 * session is failed and torn down, releasing its charges. Thread-safe; call
 * before serving. This is an additional bound on top of each stream's
 * HTTP2_MAX_BODY and any per-session budget. */
void http2_set_global_unary_budget(size_t bytes);

/* Per-session cap on unary request-body buffers. 0 = unlimited (default).
 * Exceeding either this or the process-wide budget makes http2_session_feed()
 * fail, so the caller tears the connection down and its charges are released.
 * Owner thread only. Returns 0, or -1 if `s` is NULL. */
int http2_session_set_unary_budget(http2_session_t *s, size_t bytes);

/* Unary body bytes currently charged to this session. Owner thread only. */
size_t http2_session_unary_bytes(const http2_session_t *s);

/* Monotonic count of inbound application frames (request HEADERS and DATA).
 * Control frames, SETTINGS, and PING ACKs do not advance it. Compare two
 * snapshots to detect application traffic independently of keepalive.
 * Owner thread only. */
uint64_t http2_session_app_activity(const http2_session_t *s);

/* Push freshly-read wire bytes into the session. Returns 0 on success,
 * -1 on session error (caller should tear the connection down).
 */
int http2_session_feed(http2_session_t *s, const uint8_t *data, size_t len);

/* Destroy a session, freeing all stream state (incl. queued payloads). */
void http2_session_free(http2_session_t *s);

/* Release this thread's internal object pools. Call on a worker thread after
 * all of its sessions are gone (e.g., at graceful shutdown) so the pooled
 * blocks are actually returned to the allocator. Safe to call repeatedly. */
void http2_thread_cleanup(void);

/* Graceful drain, phase 1: submit a GOAWAY with last_stream_id (1u<<31)-1 via
 * nghttp2's shutdown notice. Existing streams keep running and the peer may
 * still open new ones; call this, wait roughly one RTT, then call
 * http2_session_goaway() to pin the final stream id. Returns 0/-1. */
int http2_session_shutdown_notice(http2_session_t *s);

/* Graceful drain, phase 2: submit a final GOAWAY whose last_stream_id is the
 * highest stream already processed. Streams above it are closed and new
 * request HEADERS are ignored by nghttp2; streams at or below it finish
 * normally. After they complete, nghttp2_session_want_read/_write both return 0
 * and the connection may be closed. Returns 0/-1. */
int http2_session_goaway(http2_session_t *s, uint32_t error_code);

/* Number of streams still tracked by the session (requests not yet closed).
 * Zero means the connection is idle and safe to time out. */
size_t http2_session_active_streams(const http2_session_t *s);

/* Keepalive PING: submit a PING carrying the 8-byte `opaque` token. The caller
 * owns the retry/timeout policy; use http2_session_ping_ack() to learn when the
 * peer acknowledged it. Intended usage is one outstanding PING at a time.
 * Submit-only: call http2_flush() (or let the feed/flush path run) to send it.
 * Returns 0/-1. */
int http2_session_ping(http2_session_t *s, uint64_t opaque);

/* Consume a received PING ACK, if any: returns 1 and stores the echoed token in
 * *opaque (when non-NULL), or 0 if no ACK arrived since the last call. */
int http2_session_ping_ack(http2_session_t *s, uint64_t *opaque);

/* The stream's one-time params region (streaming, prefix_len > 0), borrowed for
 * the stream's life. Sets *len to prefix_len and returns the bytes, or NULL
 * (and *len 0) when there is none. Only meaningful inside the handler. */
const uint8_t *http2_prefix(const http2_stream_t *st, size_t *len);

/* Borrowed header lookup by exact name (HPACK values are binary: compare
 * namelen bytes, don't assume NUL). Returns the value pointer and sets *vlen,
 * or NULL if absent. */
const uint8_t *http2_header_get(const http2_stream_t *st, const char *name,
                                size_t namelen, size_t *vlen);

/* Open a reply with an explicit status and optional extra response headers
 * (`headers` may be NULL; nghttp2 copies every name/value, so they need not
 * outlive the call). The module mints :status from `status` (100..599) itself.
 * Then append messages with http2_respond_msg() and finish with
 * http2_respond_end(). Returns 0, or -1 if the stream is unknown, already
 * answered, or submission fails (a submission failure resets the stream). */
int http2_respond_begin(http2_session_t *s, int32_t sid, int status,
                        const nghttp2_nv *headers, size_t nheaders);

/* Append one reply message. On the first call the response is opened (status 200
 * + the queue-draining provider) unless http2_respond_begin() opened it already;
 * each call copies `body` into the stream's outbox and queues it. `end_stream`
 * marks END_STREAM on this message. body may be borrowed (it is copied; len == 0
 * queues no bytes). No bytes move until a flush. Returns 0, or -1 if the stream
 * is unknown/closed or already answered (in which case the stream is reset). The
 * building block for unary (one call, end=1), streaming (many calls), and
 * managed-side producers. */
int http2_respond_msg(http2_session_t *s, int32_t sid, const uint8_t *body,
                      size_t len, int end_stream);

/* Zero-copy variant of http2_respond_msg: takes OWNERSHIP of `body` instead of
 * copying it. `body` must be a malloc-compatible pointer (freed with free());
 * passing a borrowed/stack pointer here corrupts the heap. Ownership transfers
 * to the session, which frees it exactly once -- after the final byte is sent,
 * or on stream reset/close. A NULL body with len > 0 is rejected (and freed);
 * a NULL or empty body with len == 0 behaves like http2_respond_msg(s,sid,NULL,
 * 0,end_stream). Use this to hand over a buffer you already own (a pool block,
 * an mmap, an assembled reply) and skip the copy the borrowed variant makes. */
int http2_respond_msg_owned(http2_session_t *s, int32_t sid, uint8_t *body,
                            size_t len, int end_stream);

/* End a reply with END_STREAM, opening it first if nothing was sent yet. Empty
 * reply (no prior message) => HEADERS with END_STREAM alone. Returns 0/-1. */
int http2_respond_end(http2_session_t *s, int32_t sid);

/* Finish a reply with trailer fields instead of a bare END_STREAM. The module
 * deep-copies `nva` (which must contain no pseudo-headers and may be borrowed),
 * sends the queued body normally, then emits the trailers as a final HEADERS
 * frame carrying END_STREAM. Call this instead of a final
 * http2_respond_msg(..., end_stream=1) / http2_respond_end(); once called, the
 * stream accepts no more body writes. Opens a 200 reply first if nothing was
 * sent yet. `nvlen == 0` sends an empty trailer block (still END_STREAM).
 * Returns 0, or -1 if the stream is unknown/already finished, or the trailers
 * cannot be queued (a queueing failure resets the stream). */
int http2_respond_trailers(http2_session_t *s, int32_t sid,
                           const nghttp2_nv *nva, size_t nvlen);

/* Reset a stream with PROTOCOL_ERROR. Submit-only: the bytes go out on the
 * next flush (feed's trailing send, or http2_flush). For other error codes call
 * nghttp2_submit_rst_stream() via http2_raw(). */
void http2_rst_stream(http2_session_t *s, int32_t sid);

/* Escape hatches: call any nghttp2_submit_* / query API directly,
 * then http2_flush() to push the bytes out. */
nghttp2_session *http2_raw(http2_session_t *s);
int http2_flush(http2_session_t *s);

#endif
