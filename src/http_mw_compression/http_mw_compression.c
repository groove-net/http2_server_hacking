#include "http_mw_compression.h"
#include "compress.h"
#include <stdlib.h>
#include <string.h>

typedef struct {
  compress_ctx_t *decoder;
  compress_ctx_t *encoder;
  uint8_t *out;
  size_t out_len;
  size_t out_cap;
  int decoder_done;
} comp_state_t;

static compress_algorithm_t alg(http2_compression_t c) {
  return c == HTTP2_COMPRESSION_ZSTD ? COMPRESS_ZSTD
         : c == HTTP2_COMPRESSION_BROTLI ? COMPRESS_BROTLI : COMPRESS_GZIP;
}
static compress_profile_t prof(http2_compression_profile_t p) {
  return p == HTTP2_COMPRESSION_BEST ? COMPRESS_BEST
         : p == HTTP2_COMPRESSION_FAST ? COMPRESS_FAST : COMPRESS_BALANCED;
}
static int ensure(http_exchange_t *x, comp_state_t *s, size_t add) {
  if (s->out_len + add <= s->out_cap) return 0;
  size_t cap = s->out_cap ? s->out_cap : 4096;
  while (cap < s->out_len + add) cap *= 2;
  uint8_t *p = realloc(s->out, cap);
  if (p == NULL) return -1;
  s->out = p;
  s->out_cap = cap;
  (void)x;
  return 0;
}
static comp_state_t *state(http_exchange_t *x, int create) {
  comp_state_t *s = x->compression_state;
  if (s != NULL || !create) return s;
  s = calloc(1, sizeof *s);
  if (s == NULL) return NULL;
  if (x->compression != HTTP2_COMPRESSION_OFF) {
    s->decoder = compress_decoder_new(alg(x->compression), x->max_decompressed);
    s->encoder = compress_encoder_new(alg(x->compression),
                                      prof(x->compression_profile));
    if (s->decoder == NULL || s->encoder == NULL) {
      compress_ctx_free(s->decoder); compress_ctx_free(s->encoder);
      free(s); return NULL;
    }
  }
  x->compression_state = s;
  return s;
}
static int on_request(http_exchange_t *x) {
  if (x->compression == HTTP2_COMPRESSION_OFF) return HTTP_CONTINUE;
  return state(x, 1) != NULL ? HTTP_CONTINUE : HTTP_ERROR;
}
static int on_data(http_exchange_t *x, http_span_t *span, int end_stream) {
  (void)end_stream;
  comp_state_t *s = state(x, 0);
  if (s == NULL || s->decoder == NULL || span->len == 0) return HTTP_CONTINUE;
  const uint8_t *in = span->data; size_t len = span->len, off = 0;
  s->out_len = 0;
  while (off < len) {
    uint8_t buf[16384]; size_t iu = 0, ou = 0;
    compress_status_t st = compress_ctx_run(s->decoder, in + off, len - off,
                                            &iu, buf, sizeof buf, &ou, 0);
    off += iu;
    if (ensure(x, s, ou) != 0) return HTTP_ERROR;
    memcpy(s->out + s->out_len, buf, ou); s->out_len += ou;
    if (st == COMPRESS_DONE) { s->decoder_done = 1; break; }
    if (st == COMPRESS_ERROR || st == COMPRESS_LIMIT) {
      x->status = 400;
      return HTTP_STOP;
    }
    if (iu == 0 && ou == 0) break;
  }
  span->data = s->out; span->len = s->out_len;
  return HTTP_CONTINUE;
}
static int on_response(http_exchange_t *x) {
  comp_state_t *s = state(x, 0);
  if (s == NULL || s->encoder == NULL || x->resp_body_len == 0)
    return HTTP_CONTINUE;
  const uint8_t *in = x->resp_body; size_t len = x->resp_body_len, off = 0;
  s->out_len = 0;
  for (;;) {
    uint8_t buf[16384]; size_t iu = 0, ou = 0;
    compress_status_t st = compress_ctx_run(s->encoder, in + off, len - off,
                                            &iu, buf, sizeof buf, &ou, 1);
    off += iu;
    if (ensure(x, s, ou) != 0) return HTTP_ERROR;
    memcpy(s->out + s->out_len, buf, ou); s->out_len += ou;
    if (st == COMPRESS_DONE) break;
    if (st == COMPRESS_ERROR || st == COMPRESS_LIMIT) return HTTP_ERROR;
    if (iu == 0 && ou == 0) break;
  }
  x->resp_body = s->out; x->resp_body_len = s->out_len;
  return HTTP_CONTINUE;
}
static int on_close(http_exchange_t *x) {
  comp_state_t *s = x->compression_state;
  if (s != NULL) {
    compress_ctx_free(s->decoder); compress_ctx_free(s->encoder);
    free(s->out); free(s); x->compression_state = NULL;
  }
  return HTTP_CONTINUE;
}
void http_mw_compression_init(http_mw_compression_t *mw) {
  memset(mw, 0, sizeof *mw);
  mw->stage.name = "compression";
  mw->stage.on_request = on_request;
  mw->stage.on_data = on_data;
  mw->stage.on_response = on_response;
  mw->stage.on_close = on_close;
}
const http_stage_t *http_mw_compression_stage(http_mw_compression_t *mw) {
  return mw != NULL ? &mw->stage : NULL;
}
