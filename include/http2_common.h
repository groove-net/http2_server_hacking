#ifndef HTTP2_COMMON_H
#define HTTP2_COMMON_H

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

struct iovec;

/* Caps against unbounded peer-driven growth. Tweak as needed. */
enum {
  HTTP2_DEFAULT_WINDOW_SIZE = 65535,        /* HTTP/2 initial receive window */
  HTTP2_MAX_HEADERS = 128,                  /* header fields stored per stream */
  HTTP2_MAX_BODY = 4u * 1024u * 1024u,     /* unary body buffered per stream */
  HTTP2_MAX_RESIDUAL = 1u * 1024u * 1024u, /* max streaming message/fixed buffer */
  HTTP2_BATCH_MAX = 64, /* streaming: messages handed to the handler per call */
  HTTP2_DEFAULT_STREAM_QUEUE = 4u * 1024u * 1024u,
  HTTP2_DEFAULT_CONNECTION_QUEUE = 16u * 1024u * 1024u,
  HTTP2_DEFAULT_TENANT_QUEUE = 64u * 1024u * 1024u,
  HTTP2_DEFAULT_GLOBAL_QUEUE = 256u * 1024u * 1024u,
  HTTP2_MAX_DECOMPRESSED = 64u * 1024u * 1024u,
};

/* Static per-route compression. The route contract is shared by both peers;
 * no Content-Encoding or Accept-Encoding headers are used. */
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

/* One framed message in a streaming batch. Borrowed for the handler call only
 * and may point into chunk memory; copy anything you retain. */
typedef struct http2_msg {
  const uint8_t *msg;
  size_t len;
} http2_msg_t;

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

#endif
