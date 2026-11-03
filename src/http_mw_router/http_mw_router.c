#include "http_mw_router.h"
#include <string.h>

static int on_request(http_exchange_t *x) {
  http_mw_router_t *mw = x->stages[x->req_index].state;
  if (mw == NULL) return HTTP_ERROR;
  for (size_t i = 0; i < mw->nentries; i++) {
    const http_route_entry_t *e = &mw->entries[i];
    if (strcmp(e->path, x->path) == 0 &&
        (e->method == NULL || strcmp(e->method, x->method) == 0)) {
      x->handler = e->handler;
      x->handler_ctx = e->ctx;
      x->compression = e->compression;
      x->compression_profile = e->compression_profile;
      x->max_decompressed = e->max_decompressed != 0
                                ? e->max_decompressed
                                : (64u << 20);
      return HTTP_CONTINUE;
    }
  }
  x->status = 404;
  return HTTP_STOP;
}

void http_mw_router_init(http_mw_router_t *mw,
                         const http_route_entry_t *entries, size_t nentries) {
  if (mw == NULL) return;
  memset(mw, 0, sizeof *mw);
  mw->stage.name = "router";
  mw->stage.on_request = on_request;
  mw->stage.state = mw;
  mw->entries = entries;
  mw->nentries = nentries;
}

const http_stage_t *http_mw_router_stage(http_mw_router_t *mw) {
  return mw != NULL ? &mw->stage : NULL;
}
