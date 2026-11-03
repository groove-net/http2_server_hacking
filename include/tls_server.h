#ifndef TLS_H
#define TLS_H

/* TLS server transport with ALPN "h2" on top of OpenSSL memory BIOs.
 *
 *   1. accept a non-blocking fd and create a tls_conn_t;
 *   2. read encrypted bytes from the socket, feed them with tls_conn_feed(),
 *      and call tls_conn_handshake() until it returns 1;
 *   3. after the handshake, keep feeding encrypted input, use tls_conn_read()
 *      to obtain plaintext for the HTTP/2 layer;
 *   4. for responses, hand plaintext output back with
 * tls_conn_write()/tls_conn_writev();
 *   5. whenever the fd is writable, call tls_conn_flush() to push pending
 *      encrypted bytes;
 *   6. free the connection when the socket is finished.
 *
 * The memory-BIO design means this module never reads or writes the socket
 * except in tls_conn_flush().
 *
 * Ownership/lifetime:
 * - tls_config_t is the shared SSL_CTX. Create it once before serving and free
 *   it only after every connection using it is gone.
 * - tls_conn_t is per connection and is not thread-safe. Drive it from one
 *   thread at a time, normally the worker that owns the socket.
 * - A tls_conn_t does not own or close the fd. tls_conn_free() releases only
 *   OpenSSL state and internal output storage.
 * - All input/output buffers are borrowed for the duration of the call unless
 *   explicitly stated otherwise. Encrypted output produced by OpenSSL is
 *   kept in OpenSSL's bounded write BIO until flushed.
 *
 * Return conventions:
 * - ssize_t functions return >= 0 for progress or "retry later"; -1 is fatal.
 * - A return of 0 is not necessarily an error: it can mean
 * WANT_READ/WANT_WRITE, an empty output queue, or a full output buffer. Inspect
 * the connection state and retry after the fd becomes ready.
 * - tls_conn_write()/tls_conn_writev() return the number of plaintext bytes
 *   accepted, which may be less than requested. The caller must retry the
 *   remainder from the returned byte count.
 * - After any operation that can produce encrypted bytes, call
 *   tls_conn_flush() when the fd is writable.
 */

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/uio.h>

typedef struct tls_config tls_config_t;
typedef struct tls_conn tls_conn_t;

/* Create a server configuration from PEM certificate-chain and private-key
 * files. ALPN selects "h2" only; TLS 1.2 is the minimum supported version.
 * Returns NULL on bad arguments, OpenSSL initialization failure, certificate
 * load failure, key load failure, or key/certificate mismatch. The result is
 * shared by connections and must outlive them. */
tls_config_t *tls_config_new(const char *cert_file, const char *key_file);

/* Set the ALPN protocols advertised/selected. `protos` is a length-prefixed
 * concatenation (e.g. "\x02h2"). Empty disables ALPN. */
int tls_config_set_alpn(tls_config_t *cfg, const uint8_t *protos, size_t len);

/* Release a configuration. Do not call while any tls_conn_t created from it is
 * still alive. */
void tls_config_free(tls_config_t *cfg);

/* Create a per-connection TLS state for an already-connected, non-blocking fd.
 * The fd is used only by tls_conn_flush(); this call performs no I/O and does
 * not take ownership of the fd. Returns NULL on allocation/OpenSSL failure. */
tls_conn_t *tls_conn_new(tls_config_t *cfg, int fd);

/* Release per-connection state and buffered encrypted output. Does not close
 * the fd and does not implicitly send close_notify; call tls_conn_shutdown()
 * and tls_conn_flush() first if a graceful TLS close is required. */
void tls_conn_free(tls_conn_t *tls);

/* Drive the server handshake after feeding input. Call repeatedly as encrypted
 * bytes arrive or the fd becomes writable. Returns:
 *   1  handshake complete and ALPN "h2" negotiated;
 *   0  need more input or output progress; check tls_conn_wants_read() and
 *      tls_conn_pending_out(), then flush and/or read more;
 *  -1  fatal TLS failure, including missing/incorrect ALPN.
 * Handshake output may be pending even on return 0. Always flush pending output
 * before waiting for writability. */
int tls_conn_handshake(tls_conn_t *tls);

/* 1 if the handshake completed, 0 otherwise. */
int tls_conn_handshake_done(const tls_conn_t *tls);

/* Feed encrypted bytes read from the socket into OpenSSL. Returns the number
 * of bytes accepted, or -1 on fatal error. The caller may need to retry any
 * unaccepted suffix. This performs no socket I/O. */
ssize_t tls_conn_feed(tls_conn_t *tls, const uint8_t *data, size_t len);

/* Decrypt application data into buf. Call only after the handshake completes.
 * Returns:
 *   >0  plaintext bytes written to buf;
 *    0  no plaintext currently available, or close_notify/EOF;
 *   -1  fatal TLS error.
 * Use tls_conn_is_eof() to distinguish EOF from a temporary WANT_READ. The
 * operation may also generate encrypted control data; flush it afterwards. */
ssize_t tls_conn_read(tls_conn_t *tls, uint8_t *buf, size_t len);

/* Encrypt plaintext into OpenSSL's bounded write BIO. Returns the number
 * of plaintext bytes accepted (>0), 0 if no progress is possible now (for
 * example the write BIO is full or OpenSSL needs input), or -1 on fatal
 * error. Partial acceptance is normal: retry the remainder later. Call
 * tls_conn_flush() to move encrypted bytes to the socket. */
ssize_t tls_conn_write(tls_conn_t *tls, const uint8_t *data, size_t len);

/* writev-style variant of tls_conn_write(). Treats the iovecs as one logical
 * buffer and returns the total plaintext bytes accepted from the front. Stops
 * at the first iovec that cannot be fully accepted. Returns 0 if no bytes were
 * accepted, or -1 for bad arguments/fatal TLS error. */
ssize_t tls_conn_writev(tls_conn_t *tls, const struct iovec *iov, int iovcnt);

/* Write as much pending encrypted output to the fd as possible without
 * blocking. Returns the number of bytes written (0 if nothing was pending or
 * the socket is not writable), or -1 on fatal socket/TLS error. If
 * tls_conn_pending_out() is still nonzero afterwards, retry when writable. */
ssize_t tls_conn_flush(tls_conn_t *tls);

/* Begin a graceful TLS shutdown by generating close_notify (no-op before the
 * handshake completes). Returns the OpenSSL shutdown result: 1 when complete,
 * 0 when waiting for the peer's close_notify, or -1 on error. Generated output
 * is buffered until tls_conn_flush(). Does not free the connection. */
int tls_conn_shutdown(tls_conn_t *tls);

/* Encrypted bytes waiting to be written to the fd. */
size_t tls_conn_pending_out(const tls_conn_t *tls);

/* 1 if the last handshake/read/write operation needs more encrypted input,
 * 0 otherwise. This is advisory; the caller still drives readiness. */
int tls_conn_wants_read(const tls_conn_t *tls);

/* 1 if close_notify/EOF was observed, 0 otherwise. */
int tls_conn_is_eof(const tls_conn_t *tls);

#endif
