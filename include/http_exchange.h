#ifndef HTTP_EXCHANGE_H
#define HTTP_EXCHANGE_H
#include "http2_common.h"
#include <stddef.h>
#include <stdint.h>

typedef struct http_exchange http_exchange_t;
typedef void (*http_handler_fn)(http_exchange_t *);
typedef int (*http_stage_fn)(http_exchange_t *);
typedef struct {
  const uint8_t *data;
  size_t len;
} http_span_t;
typedef int (*http_data_fn)(http_exchange_t *, http_span_t *, int);

enum { HTTP_CONTINUE = 0, HTTP_STOP = 1, HTTP_PENDING = 2, HTTP_ERROR = -1 };

typedef struct {
  const char *name;
  http_stage_fn on_request;
  http_data_fn on_data;
  http_stage_fn on_end;
  http_stage_fn on_response;
  http_stage_fn on_close;
  void *state;
} http_stage_t;

struct http_exchange {
  const char *method;
  const char *path;
  char method_storage[16];
  char path_storage[256];
  const http2_header_t *headers;
  size_t nheaders;
  const uint8_t *req_body;
  size_t req_body_len;
  int status;
  const uint8_t *resp_body;
  size_t resp_body_len;
  size_t resp_charged;
  size_t limit_body_bytes;
  size_t limit_queue_bytes;
  const http_stage_t *stages;
  size_t nstages;
  size_t req_index;
  http_handler_fn handler;
  void *handler_ctx;
  void *app_state;
  size_t wire_credit_owed;
  int streaming;
  int response_sent;
  size_t body_limit;
  size_t resp_limit;
  http2_compression_t compression;
  http2_compression_profile_t compression_profile;
  size_t max_decompressed;
  void *compression_state;
};

http_stage_fn http_exchange_stage(const http_stage_t *stage);
int http_exchange_request(http_exchange_t *x);
int http_exchange_response(http_exchange_t *x);
int http_exchange_run(http_exchange_t *x);
int http_exchange_data(http_exchange_t *x, const uint8_t *data, size_t len,
                       int end_stream);
int http_exchange_end(http_exchange_t *x);
void http_exchange_close(http_exchange_t *x);
const char *http_exchange_header(const http_exchange_t *x, const char *name);
#endif
