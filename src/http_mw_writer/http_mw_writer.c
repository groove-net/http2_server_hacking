#include "http_mw_writer.h"
#include <string.h>

static int on_response(http_exchange_t *x) {
  if (x->resp_body_len == 0) return HTTP_CONTINUE;
  if (x->resp_limit != 0 && x->resp_body_len > x->resp_limit) {
    x->resp_body = NULL;
    x->resp_body_len = 0;
    x->status = 413;
    return HTTP_STOP;
  }
  return HTTP_CONTINUE;
}


void http_mw_writer_init(http_mw_writer_t *mw, size_t max_response) {
  if (mw == NULL) return;
  memset(mw, 0, sizeof *mw);
  mw->stage.name = "writer";
  mw->stage.on_response = on_response;
  mw->max_response = max_response;
}

const http_stage_t *http_mw_writer_stage(http_mw_writer_t *mw) {
  return mw != NULL ? &mw->stage : NULL;
}

int http_mw_writer_submit(http2_session_t *s, http2_stream_t *st,
                          http_exchange_t *x) {
  if (s == NULL || st == NULL || x == NULL) return -1;
  int status = x->status != 0 ? x->status : 200;
  if (http2_respond_begin(s, http2_stream_id(st), status, NULL, 0) != 0)
    return -1;
  if (x->resp_body_len > 0)
    return http2_respond_msg_ref(s, http2_stream_id(st), x->resp_body,
                                 x->resp_body_len, 1);
  return http2_respond_end(s, http2_stream_id(st));
}
