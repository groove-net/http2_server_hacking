#ifndef HTTP_MW_ROUTER_H
#define HTTP_MW_ROUTER_H
#include "http_exchange.h"
typedef struct {
  const char *method;
  const char *path;
  http_handler_fn handler;
  void *ctx;
  http2_compression_t compression;
  http2_compression_profile_t compression_profile;
  size_t max_decompressed;
} http_route_entry_t;
typedef struct http_mw_router {
  http_stage_t stage;
  const http_route_entry_t *entries;
  size_t nentries;
} http_mw_router_t;
void http_mw_router_init(http_mw_router_t *mw,
                         const http_route_entry_t *entries,
                         size_t nentries);
const http_stage_t *http_mw_router_stage(http_mw_router_t *mw);
#endif
