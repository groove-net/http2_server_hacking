#include "http_exchange.h"
#include <string.h>

http_stage_fn http_exchange_stage(const http_stage_t *stage) {
  return stage != NULL ? stage->on_request : NULL;
}

const char *http_exchange_header(const http_exchange_t *x, const char *name) {
  if (x == NULL || name == NULL) return NULL;
  size_t n = strlen(name);
  for (size_t i = 0; i < x->nheaders; i++)
    if (x->headers[i].namelen == n &&
        memcmp(x->headers[i].name, name, n) == 0)
      return (const char *)x->headers[i].value;
  return NULL;
}

static int run_request(http_exchange_t *x) {
  while (x->req_index < x->nstages) {
    const http_stage_t *st = &x->stages[x->req_index];
    if (st->on_request == NULL) { x->req_index++; continue; }
    int rc = st->on_request(x);
    if (rc == HTTP_CONTINUE) { x->req_index++; continue; }
    return rc;
  }
  return HTTP_CONTINUE;
}

static void run_response(http_exchange_t *x) {
  for (size_t i = x->nstages; i > 0; i--)
    if (x->stages[i - 1].on_response != NULL)
      x->stages[i - 1].on_response(x);
}

int http_exchange_data(http_exchange_t *x, const uint8_t *data, size_t len,
                       int end_stream) {
  if (x == NULL) return HTTP_ERROR;
  http_span_t span = {data, len};
  for (size_t i = 0; i < x->nstages; i++) {
    const http_stage_t *st = &x->stages[i];
    if (st->on_data == NULL) continue;
    int rc = st->on_data(x, &span, end_stream);
    if (rc != HTTP_CONTINUE) return rc;
    if (span.len == 0) return HTTP_CONTINUE; /* no output for downstream */
  }
  return HTTP_CONTINUE;
}

int http_exchange_end(http_exchange_t *x) {
  if (x == NULL) return HTTP_ERROR;
  for (size_t i = 0; i < x->nstages; i++)
    if (x->stages[i].on_end != NULL && x->stages[i].on_end(x) != HTTP_CONTINUE)
      return HTTP_STOP;
  return HTTP_CONTINUE;
}

void http_exchange_close(http_exchange_t *x) {
  if (x == NULL) return;
  for (size_t i = x->nstages; i > 0; i--)
    if (x->stages[i - 1].on_close != NULL) x->stages[i - 1].on_close(x);
}

int http_exchange_request(http_exchange_t *x) {
  return x != NULL ? run_request(x) : HTTP_ERROR;
}

int http_exchange_response(http_exchange_t *x) {
  if (x == NULL) return HTTP_ERROR;
  run_response(x);
  return HTTP_CONTINUE;
}

int http_exchange_run(http_exchange_t *x) {
  if (x == NULL) return HTTP_ERROR;
  int rc = http_exchange_request(x);
  if (rc == HTTP_PENDING) return HTTP_PENDING;
  if (rc == HTTP_CONTINUE && !x->streaming) {
    rc = http_exchange_data(x, x->req_body, x->req_body_len, 1);
    if (rc == HTTP_CONTINUE) rc = http_exchange_end(x);
  }
  if (rc == HTTP_CONTINUE && x->handler != NULL) x->handler(x);
  else if (rc == HTTP_ERROR) x->status = 500;
  http_exchange_response(x);
  return rc == HTTP_ERROR ? HTTP_ERROR : HTTP_CONTINUE;
}
