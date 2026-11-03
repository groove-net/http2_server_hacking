#include "http_mw_limits.h"
#include "http2_common.h"
#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#define LIMIT_TENANT_BUCKETS 256
typedef struct limit_tenant {
  uint64_t id;
  size_t bytes;
  struct limit_tenant *next;
} limit_tenant_t;

static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;
static limit_tenant_t *g_tenants[LIMIT_TENANT_BUCKETS];
static size_t g_global_queued;
static size_t g_global_limit = HTTP2_DEFAULT_GLOBAL_QUEUE;
static size_t g_tenant_limit = HTTP2_DEFAULT_TENANT_QUEUE;
static _Atomic size_t g_unary_bytes;
static _Atomic size_t g_unary_budget;

static uint32_t bucket(uint64_t id) {
  id ^= id >> 33; id *= 0xff51afd7ed558ccdULL; id ^= id >> 33;
  return (uint32_t)(id & (LIMIT_TENANT_BUCKETS - 1));
}
void http_mw_limits_set_global_queue(size_t bytes) {
  pthread_mutex_lock(&g_mu);
  g_global_limit = bytes ? bytes : HTTP2_DEFAULT_GLOBAL_QUEUE;
  pthread_mutex_unlock(&g_mu);
}
void http_mw_limits_set_tenant_queue(size_t bytes) {
  pthread_mutex_lock(&g_mu);
  g_tenant_limit = bytes ? bytes : HTTP2_DEFAULT_TENANT_QUEUE;
  pthread_mutex_unlock(&g_mu);
}
size_t http_mw_limits_global_queue_limit(void) { return g_global_limit; }
size_t http_mw_limits_tenant_queue_limit(void) { return g_tenant_limit; }
int http_mw_limits_queue_charge(uint64_t tenant, size_t n) {
  if (n == 0) return 0;
  pthread_mutex_lock(&g_mu);
  if (g_global_queued > g_global_limit || n > g_global_limit - g_global_queued) {
    pthread_mutex_unlock(&g_mu); return -1;
  }
  limit_tenant_t *e = NULL; uint32_t b = 0; int created = 0;
  if (tenant != 0) {
    b = bucket(tenant);
    for (e = g_tenants[b]; e != NULL; e = e->next) if (e->id == tenant) break;
    if (e == NULL) {
      e = calloc(1, sizeof *e);
      if (e == NULL) { pthread_mutex_unlock(&g_mu); return -1; }
      e->id = tenant; e->next = g_tenants[b]; g_tenants[b] = e; created = 1;
    }
    if (e->bytes > g_tenant_limit || n > g_tenant_limit - e->bytes) {
      if (created) { g_tenants[b] = e->next; free(e); }
      pthread_mutex_unlock(&g_mu); return -1;
    }
  }
  g_global_queued += n; if (e != NULL) e->bytes += n;
  pthread_mutex_unlock(&g_mu); return 0;
}
void http_mw_limits_queue_release(uint64_t tenant, size_t n) {
  if (n == 0) return;
  pthread_mutex_lock(&g_mu);
  g_global_queued = n <= g_global_queued ? g_global_queued - n : 0;
  if (tenant != 0) {
    uint32_t b = bucket(tenant); limit_tenant_t **link = &g_tenants[b];
    while (*link != NULL) {
      limit_tenant_t *e = *link;
      if (e->id == tenant) {
        e->bytes = n <= e->bytes ? e->bytes - n : 0;
        if (e->bytes == 0) { *link = e->next; free(e); }
        break;
      }
      link = &e->next;
    }
  }
  pthread_mutex_unlock(&g_mu);
}
void http_mw_limits_set_unary_budget(size_t bytes) {
  atomic_store_explicit(&g_unary_budget, bytes, memory_order_relaxed);
}
int http_mw_limits_unary_charge(size_t n) {
  if (n == 0) return 0;
  size_t limit = atomic_load_explicit(&g_unary_budget, memory_order_relaxed);
  if (limit == 0) { atomic_fetch_add_explicit(&g_unary_bytes, n, memory_order_relaxed); return 0; }
  size_t cur = atomic_load_explicit(&g_unary_bytes, memory_order_relaxed);
  for (;;) {
    if (cur > limit || n > limit - cur) return -1;
    if (atomic_compare_exchange_weak_explicit(&g_unary_bytes, &cur, cur + n,
        memory_order_relaxed, memory_order_relaxed)) return 0;
  }
}
void http_mw_limits_unary_uncharge(size_t n) {
  if (n != 0) atomic_fetch_sub_explicit(&g_unary_bytes, n, memory_order_relaxed);
}

