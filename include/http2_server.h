#ifndef HTTP2_SERVER_H
#define HTTP2_SERVER_H
#include "http2_common.h"

/* Neutral HTTP/2 server layer (h2c, prior knowledge) on nghttp2.
 *
 * nghttp2 does framing + HPACK only, no I/O: the engine reads bytes off the
 * wire and pushes them here via http2_session_feed(); framed output bytes
 * go back out through the session's send callbacks.
 *
 * Inbound DATA is delivered to the route's raw handler as borrowed spans.
 * Message framing, buffering, decompression, and limits belong above this
 * layer. Outbound responses are queued through the respond family
 * (http2_respond_msg
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

struct http2_session;
struct http2_stream;
typedef struct http2_session http2_session_t;
typedef struct http2_stream http2_stream_t;

/* Raw DATA sink. Receives the DATA span as delivered by HTTP/2; framing and
 * buffering are the application/middleware layer's responsibility. */
typedef void (*http2_raw_fn)(struct http2_session *s,
                              struct http2_stream *st,
                              const uint8_t *data, size_t len);

/* Route configuration returned by the application's route resolver. HTTP/2
 * stores no route table; it asks the resolver for the current :path.
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
  size_t state_size;                /* bytes of per-stream ctx (0 = app-managed) */
  void (*state_init)(void *ctx);    /* optional ctor, run once at HEADERS */
  void (*state_free)(void *ctx);    /* optional dtor, run at stream close */
  void (*stream_end)(struct http2_session *s,
                     struct http2_stream *st); /* streaming END_STREAM */
  http2_raw_fn raw;                /* raw DATA handler (framing above core) */
  /* Optional per-route stream receive window, applied with a WINDOW_UPDATE at
   * request HEADERS. 0 keeps the session default from http2_session_new(). */
  uint32_t initial_window;
  http2_flow_profile_t flow_profile; /* INHERIT = session profile */
} http2_route_t;

typedef const http2_route_t *(*http2_route_resolver_fn)(void *ctx,
                                                       const uint8_t *path,
                                                       size_t path_len);

/* Install the application route resolver. HTTP/2 owns no route table. Must be
 * called before serving. */
void http2_set_route_resolver(http2_route_resolver_fn fn, void *ctx);

/* Stream ID and application context. The stream representation is private. */
int32_t http2_stream_id(const http2_stream_t *st);
void *http2_stream_ctx(const http2_stream_t *st);
const http2_header_t *http2_stream_headers(const http2_stream_t *st);
size_t http2_stream_header_count(const http2_stream_t *st);


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
 * passes a configured default. Flow-control windows and the outbound queue
 * bound protocol memory; request-body limits belong to the middleware layer.
 * Individual routes may override the stream window with
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

/* Per-session stream and connection queue limits. A zero limit restores the
 * corresponding default. Returns -1 if connection_limit is smaller than
 * stream_limit or the session is NULL. */
int http2_session_set_queue_limits(http2_session_t *s, size_t stream_limit,
                                   size_t connection_limit);

/* Bytes currently queued for this session's outbound responses. */
size_t http2_session_queued_bytes(const http2_session_t *s);

/* Monotonic count of inbound application frames (HEADERS and DATA). */
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

/* Open a response with a status and optional headers. */
int http2_respond_begin(http2_session_t *s, int32_t sid, int status,
                        const nghttp2_nv *headers, size_t nheaders);

/* Send a copied response body. */
int http2_respond_msg(http2_session_t *s, int32_t sid, const uint8_t *body,
                      size_t len, int end_stream);

/* Send a borrowed response body; caller keeps it alive until stream close. */
int http2_respond_msg_ref(http2_session_t *s, int32_t sid,
                          const uint8_t *body, size_t len, int end_stream);

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
