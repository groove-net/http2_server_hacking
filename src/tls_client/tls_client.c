#include "tls_client.h"
#include "liblog.h"
#include <errno.h>
#include <limits.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define OUT_MAX (4u * 1024u * 1024u)

struct tls_client_config {
  SSL_CTX *ctx;
  int insecure;
  uint8_t protos[256];
  size_t protos_len;
};
struct tls_client_conn {
  SSL *ssl; int fd; uint8_t *out; size_t off, len, cap; int done; int wants_read;
  const tls_client_config_t *cfg;
};

static void logssl(const char *w) { char b[256]; unsigned long e=ERR_get_error();
  if(e==0){log_error("[tls-client] %s failed",w);return;} ERR_error_string_n(e,b,sizeof b); log_error("[tls-client] %s failed: %s",w,b); }

tls_client_config_t *tls_client_config_new(const char *ca_file, int insecure){
  tls_client_config_t *c=calloc(1,sizeof *c); if(!c) return NULL;
  c->insecure = insecure;
  c->ctx=SSL_CTX_new(TLS_client_method()); if(!c->ctx){logssl("SSL_CTX_new");free(c);return NULL;}
  SSL_CTX_set_min_proto_version(c->ctx, TLS1_2_VERSION);
  c->protos_len = 3;
  memcpy(c->protos, "\x02h2", 3);
  if(insecure){ SSL_CTX_set_verify(c->ctx, SSL_VERIFY_NONE, NULL); }
  else{
    SSL_CTX_set_verify(c->ctx, SSL_VERIFY_PEER, NULL);
    if(ca_file && *ca_file) SSL_CTX_load_verify_locations(c->ctx, ca_file, NULL);
    else SSL_CTX_set_default_verify_paths(c->ctx);
  }
  return c;
}
int tls_client_config_set_alpn(tls_client_config_t *c, const uint8_t *protos,
                               size_t len) {
  if (c == NULL || (len > 0 && protos == NULL) || len > sizeof c->protos)
    return -1;
  c->protos_len = len;
  if (len > 0) memcpy(c->protos, protos, len);
  return 0;
}

void tls_client_config_free(tls_client_config_t *c){ if(!c)return; SSL_CTX_free(c->ctx); free(c); }

tls_client_conn_t *tls_client_conn_new(tls_client_config_t *cfg, int fd, const char *host){
  if(!cfg) return NULL;
  tls_client_conn_t *c=calloc(1,sizeof *c); if(!c) return NULL; c->fd=fd; c->cfg=cfg;
  c->ssl=SSL_new(cfg->ctx); BIO *r=BIO_new(BIO_s_mem()), *w=BIO_new(BIO_s_mem());
  if(!c->ssl||!r||!w){logssl("SSL_new/BIO_new");SSL_free(c->ssl);BIO_free(r);BIO_free(w);free(c);return NULL;}
  BIO_set_mem_eof_return(r,-1); SSL_set_bio(c->ssl,r,w); SSL_set_connect_state(c->ssl);
  SSL_set_tlsext_host_name(c->ssl, host);
  if (!cfg->insecure) {
    SSL_set1_host(c->ssl, host);
    SSL_set_verify(c->ssl, SSL_VERIFY_PEER, NULL);
  } else {
    SSL_set_verify(c->ssl, SSL_VERIFY_NONE, NULL);
  }
  if (cfg->protos_len > 0) {
    SSL_set_alpn_protos(c->ssl, cfg->protos, (unsigned int)cfg->protos_len);
  }
  return c;
}
void tls_client_conn_free(tls_client_conn_t *c){ if(!c)return; SSL_free(c->ssl); free(c->out); free(c); }

static int reserve(tls_client_conn_t *c,size_t add){ if(c->off>0){size_t l=c->len-c->off; if(l) memmove(c->out,c->out+c->off,l); c->len=l;c->off=0;} if(add>OUT_MAX||c->len>OUT_MAX-add)return -1; size_t need=c->len+add; if(need<=c->cap)return 0; size_t cap=c->cap?c->cap:4096; while(cap<need){cap=cap>OUT_MAX/2?OUT_MAX:cap*2;} uint8_t*g=realloc(c->out,cap); if(!g)return -1; c->out=g;c->cap=cap; return 0; }

static int drain(tls_client_conn_t *c){ BIO*w=SSL_get_wbio(c->ssl); for(;;){ size_t n=(size_t)BIO_ctrl_pending(w); if(!n)return 0; size_t t=n<16384?n:16384; if(reserve(c,t)!=0)return -1; int r=BIO_read(w,c->out+c->len,(int)t); if(r<=0)return 0; c->len+=(size_t)r; } }

static int waitc(tls_client_conn_t*c,int rc,const char*w){ int e=SSL_get_error(c->ssl,rc);
  if(e==SSL_ERROR_WANT_READ){c->wants_read=1;return 0;} if(e==SSL_ERROR_WANT_WRITE)return 0;
  if(e==SSL_ERROR_ZERO_RETURN)return 0; logssl(w); return -1; }

int tls_client_conn_handshake(tls_client_conn_t *c){ if(!c)return -1; if(c->done)return 1; c->wants_read=0;
  int r=SSL_connect(c->ssl); if(drain(c)!=0)return -1;
  if(r==1){
    const unsigned char *p=NULL; unsigned int n=0;
    SSL_get0_alpn_selected(c->ssl, &p, &n);
    if (c->cfg->protos_len > 0 && (p == NULL || n == 0)) {
      log_error("[tls-client] peer did not negotiate a requested ALPN protocol");
      return -1;
    }
    c->done=1; return 1;
  }
  return waitc(c,r,"SSL_connect"); }
int tls_client_conn_handshake_done(const tls_client_conn_t *c){return c&&c->done;}

ssize_t tls_client_conn_feed(tls_client_conn_t*c,const uint8_t*d,size_t n){ if(!c||(n&&!d))return -1; if(n>(size_t)INT_MAX)n=INT_MAX; BIO*r=SSL_get_rbio(c->ssl); int x=BIO_write(r,d,(int)n); return x<0?-1:(ssize_t)x; }
ssize_t tls_client_conn_read(tls_client_conn_t*c,uint8_t*b,size_t n){ if(!c||!b||!n)return -1; if(n>(size_t)INT_MAX)n=INT_MAX; c->wants_read=0; int r=SSL_read(c->ssl,b,(int)n); if(drain(c)!=0)return -1; if(r>0)return r; return waitc(c,r,"SSL_read"); }
ssize_t tls_client_conn_write(tls_client_conn_t*c,const uint8_t*d,size_t n){ if(!c||!d||!n)return -1; if(c->len-c->off>=OUT_MAX)return 0; if(n>(size_t)INT_MAX)n=INT_MAX; c->wants_read=0; int r=SSL_write(c->ssl,d,(int)n); if(drain(c)!=0)return -1; if(r>0)return r; return waitc(c,r,"SSL_write"); }
ssize_t tls_client_conn_flush(tls_client_conn_t*c){ if(!c)return -1; size_t total=0; while(c->off<c->len){ ssize_t n=write(c->fd,c->out+c->off,c->len-c->off); if(n>0){c->off+=(size_t)n;total+=(size_t)n;continue;} if(n<0&&errno==EINTR)continue; if(n<0&&(errno==EAGAIN||errno==EWOULDBLOCK))break; if(n==0)break; return -1; } if(c->off==c->len){c->off=0;c->len=0;} return (ssize_t)total; }int tls_client_conn_shutdown(tls_client_conn_t*c){ if(!c||!c->done)return 0; c->wants_read=0; int r=SSL_shutdown(c->ssl); if(drain(c)!=0)return -1; if(r<0)return waitc(c,r,"SSL_shutdown"); return r; }
size_t tls_client_conn_pending_out(const tls_client_conn_t*c){return c?c->len-c->off:0;}
int tls_client_conn_wants_read(const tls_client_conn_t*c){return c&&c->wants_read;}
int tls_client_conn_is_eof(const tls_client_conn_t*c){return c!=NULL && (SSL_get_shutdown(c->ssl)&SSL_RECEIVED_SHUTDOWN)!=0;}
