#include "http_mw_logging.h"
#include "liblog.h"

static int on_request(http_exchange_t *x) {
  log_debug("[mw] %s %s", x->method ? x->method : "?",
            x->path ? x->path : "?");
  return HTTP_CONTINUE;
}

static int on_response(http_exchange_t *x) {
  log_info("[mw] %s %s -> %d", x->method ? x->method : "?",
           x->path ? x->path : "?", x->status);
  return HTTP_CONTINUE;
}

static const http_stage_t g_stage = {
    .name = "logging", .on_request = on_request,
    .on_response = on_response, .state = NULL};

const http_stage_t *http_mw_logging_stage(void) { return &g_stage; }
