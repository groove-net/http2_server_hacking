#ifndef HTTP_MW_COMPRESSION_H
#define HTTP_MW_COMPRESSION_H
#include "http_exchange.h"
typedef struct { http_stage_t stage; } http_mw_compression_t;
void http_mw_compression_init(http_mw_compression_t *mw);
const http_stage_t *http_mw_compression_stage(http_mw_compression_t *mw);
#endif
