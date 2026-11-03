#include "http_mw_body_limits.h"
#include "http_mw_limits.h"
#include <string.h>

static int on_data(http_exchange_t *x, http_span_t *span, int end) {
  (void)end;
  if (span->len == 0) return HTTP_CONTINUE;
  if (http_mw_limits_unary_charge(span->len) != 0) {
    x->status = 503;
    return HTTP_STOP;
  }
  x->limit_body_bytes += span->len;
  return HTTP_CONTINUE;
}

static int on_close(http_exchange_t *x) {
  if (x->limit_body_bytes != 0) {
    http_mw_limits_unary_uncharge(x->limit_body_bytes);
    x->limit_body_bytes = 0;
  }
  return HTTP_CONTINUE;
}

void http_mw_body_limits_init(http_mw_body_limits_t *mw) {
  memset(mw, 0, sizeof *mw);
  mw->stage.name = "body-limits";
  mw->stage.on_data = on_data;
  mw->stage.on_close = on_close;
}

const http_stage_t *http_mw_body_limits_stage(http_mw_body_limits_t *mw) {
  return mw != NULL ? &mw->stage : NULL;
}
