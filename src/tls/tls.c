#include "../../include/tls.h"
#include "../../include/liblog.h"
#include <errno.h>
#include <limits.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define TLS_OUT_MAX (4u * 1024u * 1024u)
#define TLS_IO_CHUNK 16384

struct tls_config {
  SSL_CTX *ctx;
};

struct tls_conn {
  SSL *ssl;
  int fd;
  size_t flush_off; /* bytes already written from the current write BIO */
  int handshake_done;
  int wants_read;
  int eof;
};

static void tls_log_ssl(const char *where) {
  unsigned long e = ERR_get_error();
  char buf[256];
  if (e == 0) {
    log_error("[tls] %s failed", where);
    return;
  }
  ERR_error_string_n(e, buf, sizeof buf);
  log_error("[tls] %s failed: %s", where, buf);
}

static int tls_alpn_select(SSL *ssl, const unsigned char **out,
                           unsigned char *outlen, const unsigned char *in,
                           unsigned int inlen, void *arg) {
  (void)ssl;
  (void)arg;
  static const unsigned char protos[] = {2, 'h', '2'};
  int rc = SSL_select_next_proto((unsigned char **)out, outlen, protos,
                                 sizeof protos, in, inlen);
  if (rc != OPENSSL_NPN_NEGOTIATED) {
    return SSL_TLSEXT_ERR_ALERT_FATAL;
  }
  return SSL_TLSEXT_ERR_OK;
}

tls_config_t *tls_config_new(const char *cert_file, const char *key_file) {
  if (cert_file == NULL || key_file == NULL) {
    return NULL;
  }
  tls_config_t *cfg = calloc(1, sizeof *cfg);
  if (cfg == NULL) {
    return NULL;
  }
  cfg->ctx = SSL_CTX_new(TLS_server_method());
  if (cfg->ctx == NULL) {
    tls_log_ssl("SSL_CTX_new");
    free(cfg);
    return NULL;
  }
  SSL_CTX_set_min_proto_version(cfg->ctx, TLS1_2_VERSION);
  SSL_CTX_set_options(cfg->ctx, SSL_OP_NO_COMPRESSION | SSL_OP_NO_RENEGOTIATION);
  SSL_CTX_set_alpn_select_cb(cfg->ctx, tls_alpn_select, NULL);
  if (SSL_CTX_use_certificate_chain_file(cfg->ctx, cert_file) != 1) {
    tls_log_ssl("SSL_CTX_use_certificate_chain_file");
    SSL_CTX_free(cfg->ctx);
    free(cfg);
    return NULL;
  }
  if (SSL_CTX_use_PrivateKey_file(cfg->ctx, key_file, SSL_FILETYPE_PEM) != 1) {
    tls_log_ssl("SSL_CTX_use_PrivateKey_file");
    SSL_CTX_free(cfg->ctx);
    free(cfg);
    return NULL;
  }
  if (SSL_CTX_check_private_key(cfg->ctx) != 1) {
    tls_log_ssl("SSL_CTX_check_private_key");
    SSL_CTX_free(cfg->ctx);
    free(cfg);
    return NULL;
  }
  return cfg;
}

void tls_config_free(tls_config_t *cfg) {
  if (cfg == NULL) {
    return;
  }
  SSL_CTX_free(cfg->ctx);
  free(cfg);
}

tls_conn_t *tls_conn_new(tls_config_t *cfg, int fd) {
  if (cfg == NULL) {
    return NULL;
  }
  tls_conn_t *tls = calloc(1, sizeof *tls);
  if (tls == NULL) {
    return NULL;
  }
  tls->fd = fd;
  tls->ssl = SSL_new(cfg->ctx);
  BIO *rbio = BIO_new(BIO_s_mem());
  BIO *wbio = BIO_new(BIO_s_mem());
  if (tls->ssl == NULL || rbio == NULL || wbio == NULL) {
    tls_log_ssl("SSL_new/BIO_new");
    SSL_free(tls->ssl);
    BIO_free(rbio);
    BIO_free(wbio);
    free(tls);
    return NULL;
  }
  BIO_set_mem_eof_return(rbio, -1);
  SSL_set_bio(tls->ssl, rbio, wbio);
  SSL_set_accept_state(tls->ssl);
  SSL_set_mode(tls->ssl, SSL_MODE_ENABLE_PARTIAL_WRITE |
                              SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
  return tls;
}

void tls_conn_free(tls_conn_t *tls) {
  if (tls == NULL) {
    return;
  }
  SSL_free(tls->ssl);
  free(tls);
}

static int tls_ssl_wait(tls_conn_t *tls, int rc, const char *where) {
  int err = SSL_get_error(tls->ssl, rc);
  if (err == SSL_ERROR_WANT_READ) {
    tls->wants_read = 1;
    return 0;
  }
  if (err == SSL_ERROR_WANT_WRITE) {
    return 0;
  }
  if (err == SSL_ERROR_ZERO_RETURN) {
    tls->eof = 1;
    return 0;
  }
  tls_log_ssl(where);
  return -1;
}

int tls_conn_handshake(tls_conn_t *tls) {
  if (tls == NULL) {
    return -1;
  }
  if (tls->handshake_done) {
    return 1;
  }
  tls->wants_read = 0;
  int rc = SSL_do_handshake(tls->ssl);
  if (rc == 1) {
    const unsigned char *proto = NULL;
    unsigned int plen = 0;
    SSL_get0_alpn_selected(tls->ssl, &proto, &plen);
    if (proto == NULL || plen != 2 || memcmp(proto, "h2", 2) != 0) {
      log_error("[tls] client did not negotiate h2 via ALPN");
      return -1;
    }
    tls->handshake_done = 1;
    return 1;
  }
  return tls_ssl_wait(tls, rc, "SSL_do_handshake");
}

int tls_conn_handshake_done(const tls_conn_t *tls) {
  return tls != NULL && tls->handshake_done;
}

ssize_t tls_conn_feed(tls_conn_t *tls, const uint8_t *data, size_t len) {
  if (tls == NULL || (len > 0 && data == NULL)) {
    return -1;
  }
  if (len > (size_t)INT_MAX) {
    len = (size_t)INT_MAX;
  }
  BIO *rbio = SSL_get_rbio(tls->ssl);
  int n = BIO_write(rbio, data, (int)len);
  return n < 0 ? -1 : (ssize_t)n;
}

ssize_t tls_conn_read(tls_conn_t *tls, uint8_t *buf, size_t len) {
  if (tls == NULL || buf == NULL || len == 0) {
    return -1;
  }
  if (len > (size_t)INT_MAX) {
    len = (size_t)INT_MAX;
  }
  tls->wants_read = 0;
  int n = SSL_read(tls->ssl, buf, (int)len);
  if (n > 0) {
    return n;
  }
  return tls_ssl_wait(tls, n, "SSL_read");
}

ssize_t tls_conn_write(tls_conn_t *tls, const uint8_t *data, size_t len) {
  if (tls == NULL || data == NULL || len == 0) {
    return -1;
  }
  BIO *wbio = SSL_get_wbio(tls->ssl);
  if ((size_t)BIO_ctrl_pending(wbio) >= TLS_OUT_MAX) {
    return 0;
  }
  if (len > (size_t)INT_MAX) {
    len = (size_t)INT_MAX;
  }
  tls->wants_read = 0;
  int n = SSL_write(tls->ssl, data, (int)len);
  if (n > 0) {
    return n;
  }
  return tls_ssl_wait(tls, n, "SSL_write");
}

ssize_t tls_conn_writev(tls_conn_t *tls, const struct iovec *iov, int iovcnt) {
  if (tls == NULL || iov == NULL || iovcnt <= 0) {
    return -1;
  }
  size_t total = 0;
  for (int i = 0; i < iovcnt; i++) {
    if (iov[i].iov_len == 0) {
      continue;
    }
    ssize_t n = tls_conn_write(tls, iov[i].iov_base, iov[i].iov_len);
    if (n <= 0) {
      return total > 0 ? (ssize_t)total : n;
    }
    total += (size_t)n;
    if ((size_t)n < iov[i].iov_len) {
      break;
    }
  }
  return (ssize_t)total;
}

ssize_t tls_conn_flush(tls_conn_t *tls) {
  if (tls == NULL) {
    return -1;
  }
  BIO *wbio = SSL_get_wbio(tls->ssl);
  size_t total = 0;
  for (;;) {
    char *data = NULL;
    long pending = BIO_get_mem_data(wbio, &data);
    if (pending <= 0) {
      tls->flush_off = 0;
      return (ssize_t)total;
    }
    size_t len = (size_t)pending;
    if (tls->flush_off >= len) {
      BIO_reset(wbio);
      tls->flush_off = 0;
      return (ssize_t)total;
    }
    ssize_t n = write(tls->fd, data + tls->flush_off,
                      len - tls->flush_off);
    if (n > 0) {
      tls->flush_off += (size_t)n;
      total += (size_t)n;
      if (tls->flush_off == len) {
        BIO_reset(wbio);
        tls->flush_off = 0;
        return (ssize_t)total;
      }
      continue;
    }
    if (n < 0 && errno == EINTR) {
      continue;
    }
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      break;
    }
    if (n == 0) {
      break;
    }
    return -1;
  }
  return (ssize_t)total;
}

int tls_conn_shutdown(tls_conn_t *tls) {
  if (tls == NULL || !tls->handshake_done) {
    return 0;
  }
  int rc = SSL_shutdown(tls->ssl);
  if (rc < 0) {
    return tls_ssl_wait(tls, rc, "SSL_shutdown");
  }
  return rc;
}

size_t tls_conn_pending_out(const tls_conn_t *tls) {
  if (tls == NULL) {
    return 0;
  }
  BIO *wbio = SSL_get_wbio(tls->ssl);
  long pending = BIO_ctrl_pending(wbio);
  return pending > 0 && (size_t)pending > tls->flush_off
             ? (size_t)pending - tls->flush_off
             : 0;
}

int tls_conn_wants_read(const tls_conn_t *tls) {
  return tls != NULL && tls->wants_read;
}

int tls_conn_is_eof(const tls_conn_t *tls) {
  return tls != NULL && tls->eof;
}
