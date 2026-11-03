#include "http_mw_body.h"
#include <stdlib.h>
#include <string.h>

typedef struct {
  uint8_t *buf;
  size_t len;
  size_t cap;
  size_t charged;
  size_t max_body;
} body_state_t;

static int body_data(http_exchange_t *x, http_span_t *span, int end_stream) {
  (void)end_stream;
  const uint8_t *data = span->data;
  size_t len = span->len;
  body_state_t *b = x->app_state;
  if (len == 0) return HTTP_CONTINUE;
  if (b == NULL) {
    b = calloc(1, sizeof *b);
    if (b == NULL) return HTTP_ERROR;
    b->max_body = x->body_limit;
    x->app_state = b;
  }
  if (len > b->max_body || b->len > b->max_body - len) {
    x->status = 413;
    return HTTP_STOP;
  }
  if (b->len + len > b->cap) {
    size_t cap = b->cap ? b->cap : 1024;
    while (cap < b->len + len) cap *= 2;
    uint8_t *p = realloc(b->buf, cap);
    if (p == NULL) {
      x->status = 500;
      return HTTP_ERROR;
    }
    b->buf = p;
    b->cap = cap;
  }
  memcpy(b->buf + b->len, data, len);
  b->len += len;
  x->req_body = b->buf;
  x->req_body_len = b->len;
  return HTTP_CONTINUE;
}

static int body_close(http_exchange_t *x) {
  body_state_t *b = x->app_state;
  if (b != NULL) {
    free(b->buf);
    free(b);
    x->app_state = NULL;
  }
  return HTTP_CONTINUE;
}

void http_mw_body_init(http_mw_body_t *mw, size_t max_body) {
  if (mw == NULL) return;
  memset(mw, 0, sizeof *mw);
  mw->stage.name = "body";
  mw->stage.on_data = body_data;
  mw->stage.on_close = body_close;
  mw->max_body = max_body;
}

const http_stage_t *http_mw_body_stage(http_mw_body_t *mw) {
  return mw != NULL ? &mw->stage : NULL;
}
