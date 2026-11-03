#ifndef HTTP_MW_WRITER_H
#define HTTP_MW_WRITER_H
#include "http_exchange.h"
#include "http2_server.h"
typedef struct {
  http_stage_t stage;
  size_t max_response;
} http_mw_writer_t;
void http_mw_writer_init(http_mw_writer_t *mw, size_t max_response);
const http_stage_t *http_mw_writer_stage(http_mw_writer_t *mw);
int http_mw_writer_submit(http2_session_t *s, http2_stream_t *st,
                          http_exchange_t *x);
#endif
