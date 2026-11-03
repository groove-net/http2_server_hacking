#include "http_mw_queue_limits.h"
#include "http_mw_limits.h"
#include <string.h>

static int on_response(http_exchange_t *x) {
  if (x->resp_body_len == 0) return HTTP_CONTINUE;
  if (http_mw_limits_queue_charge(0, x->resp_body_len) != 0) {
    x->resp_body = NULL;
    x->resp_body_len = 0;
    x->status = 503;
    return HTTP_STOP;
  }
  x->limit_queue_bytes = x->resp_body_len;
  return HTTP_CONTINUE;
}

static int on_close(http_exchange_t *x) {
  if (x->limit_queue_bytes != 0) {
    http_mw_limits_queue_release(0, x->limit_queue_bytes);
    x->limit_queue_bytes = 0;
  }
  return HTTP_CONTINUE;
}

void http_mw_queue_limits_init(http_mw_queue_limits_t *mw) {
  memset(mw, 0, sizeof *mw);
  mw->stage.name = "queue-limits";
  mw->stage.on_response = on_response;
  mw->stage.on_close = on_close;
}

const http_stage_t *http_mw_queue_limits_stage(http_mw_queue_limits_t *mw) {
  return mw != NULL ? &mw->stage : NULL;
}
