#ifndef HTTP_MW_QUEUE_LIMITS_H
#define HTTP_MW_QUEUE_LIMITS_H
#include "http_exchange.h"
typedef struct { http_stage_t stage; } http_mw_queue_limits_t;
void http_mw_queue_limits_init(http_mw_queue_limits_t *mw);
const http_stage_t *http_mw_queue_limits_stage(http_mw_queue_limits_t *mw);
#endif
