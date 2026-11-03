#ifndef HTTP_MW_BODY_H
#define HTTP_MW_BODY_H
#include "http_exchange.h"
typedef struct {
  http_stage_t stage;
  size_t max_body;
} http_mw_body_t;
void http_mw_body_init(http_mw_body_t *mw, size_t max_body);
const http_stage_t *http_mw_body_stage(http_mw_body_t *mw);
#endif
