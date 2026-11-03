#ifndef TLS_CLIENT_H
#define TLS_CLIENT_H

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/uio.h>

typedef struct tls_client_config tls_client_config_t;
typedef struct tls_client_conn tls_client_conn_t;

tls_client_config_t *tls_client_config_new(const char *ca_file, int insecure);

/* Set the ALPN protocols advertised by the client. `protos` is length-prefixed
 * concatenation (e.g. "\x02h2"). Empty disables ALPN. */
int tls_client_config_set_alpn(tls_client_config_t *cfg,
                               const uint8_t *protos, size_t len);
void tls_client_config_free(tls_client_config_t *cfg);

tls_client_conn_t *tls_client_conn_new(tls_client_config_t *cfg, int fd,
                                       const char *host);
void tls_client_conn_free(tls_client_conn_t *c);

int tls_client_conn_handshake(tls_client_conn_t *c);
int tls_client_conn_handshake_done(const tls_client_conn_t *c);
ssize_t tls_client_conn_feed(tls_client_conn_t *c, const uint8_t *d, size_t n);
ssize_t tls_client_conn_read(tls_client_conn_t *c, uint8_t *b, size_t n);
ssize_t tls_client_conn_write(tls_client_conn_t *c, const uint8_t *d, size_t n);
ssize_t tls_client_conn_flush(tls_client_conn_t *c);
int tls_client_conn_shutdown(tls_client_conn_t *c);
size_t tls_client_conn_pending_out(const tls_client_conn_t *c);
int tls_client_conn_wants_read(const tls_client_conn_t *c);
int tls_client_conn_is_eof(const tls_client_conn_t *c);

#endif
