#ifndef HTTP_MW_LIMITS_H
#define HTTP_MW_LIMITS_H
#include "http_exchange.h"
#include <stddef.h>
#include <stdint.h>

void http_mw_limits_set_global_queue(size_t bytes);
void http_mw_limits_set_tenant_queue(size_t bytes);
size_t http_mw_limits_global_queue_limit(void);
size_t http_mw_limits_tenant_queue_limit(void);
int http_mw_limits_queue_charge(uint64_t tenant, size_t n);
void http_mw_limits_queue_release(uint64_t tenant, size_t n);
void http_mw_limits_set_unary_budget(size_t bytes);
int http_mw_limits_unary_charge(size_t n);
void http_mw_limits_unary_uncharge(size_t n);


#endif
