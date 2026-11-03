#ifndef HTTP2_CLIENT_H
#define HTTP2_CLIENT_H

#include "async_engine.h"
#include "http2_common.h"
#include "tls_client.h"
#include <stddef.h>
#include <stdint.h>

typedef struct http2_client_session http2_client_session_t;
typedef struct http2_client_request http2_client_request_t;

typedef struct {
  int status;
  const http2_header_t *headers;
} http2_client_response_t;

typedef void (*http2_client_unary_cb)(void *ctx,
                                      const http2_client_response_t *resp,
                                      const uint8_t *body, size_t len,
                                      int error);
typedef int (*http2_client_stream_start_cb)(void *ctx,
                                            const http2_client_response_t *resp,
                                            int error);
typedef void (*http2_client_stream_msg_cb)(void *ctx, const uint8_t *msg,
                                           size_t len, int last);
typedef ssize_t (*http2_client_body_read)(void *ctx, uint8_t *buf, size_t len,
                                        int *eof);

typedef void (*http2_client_stream_end_cb)(void *ctx,
                                           const http2_client_response_t *resp,
                                           int error);

typedef struct {
  const char *method;
  const char *path;
  const char *authority;
  const http2_header_t *headers;
  size_t nheaders;
  const uint8_t *body;
  size_t body_len;
  http2_client_body_read body_read;
  void *body_ctx;
  http2_compression_t compression;
  http2_compression_profile_t compression_profile;
  http2_msg_framing_cb response_framing;
  const void *response_framing_cfg;
  size_t max_response_message_size;
  size_t max_response_body;
} http2_client_request_opts_t;

typedef enum {
  HTTP2_CLIENT_OK = 0,
  HTTP2_CLIENT_ERR_TRANSPORT = -1,
  HTTP2_CLIENT_ERR_PROTOCOL = -2,
  HTTP2_CLIENT_ERR_LIMIT = -3,
  HTTP2_CLIENT_ERR_ABORTED = -4,
  HTTP2_CLIENT_ERR_MEMORY = -5,
} http2_client_error_t;

/* The caller owns the connected fd and drives reads/flushes. The transport is
 * optional; when NULL, writes go directly to fd. */
http2_client_session_t *
http2_client_session_new(int fd, const http2_transport_t *transport,
                         uint32_t stream_window_size,
                         uint32_t connection_window_size);
void http2_client_session_free(http2_client_session_t *s);
int http2_client_session_feed(http2_client_session_t *s, const uint8_t *data,
                              size_t len);
int http2_client_flush(http2_client_session_t *s);
int http2_client_session_want_write(const http2_client_session_t *s);
int http2_client_session_want_read(const http2_client_session_t *s);

http2_client_request_t *
http2_client_request_unary(http2_client_session_t *s,
                           const http2_client_request_opts_t *opts,
                           http2_client_unary_cb cb, void *ctx);
http2_client_request_t *http2_client_request_stream(
    http2_client_session_t *s, const http2_client_request_opts_t *opts,
    http2_client_stream_start_cb start_cb, http2_client_stream_msg_cb msg_cb,
    http2_client_stream_end_cb end_cb, void *ctx);

/* Hold automatic response-window credit until consumed. */
int http2_client_request_hold(http2_client_request_t *req);

/* Consume up to n held wire bytes; SIZE_MAX releases everything held. */
int http2_client_request_consume(http2_client_request_t *req, size_t n);

/* Resume a request body producer after body_read() deferred. */
int http2_client_request_resume(http2_client_request_t *req);

/* Cancel an in-flight request if needed, then release it. */
void http2_client_request_free(http2_client_request_t *req);


typedef struct http2_client_task http2_client_task_t;
typedef void (*http2_client_task_close_cb)(void *ctx, int error);
typedef void (*http2_client_task_ready_cb)(http2_client_task_t *task,
                                           http2_client_session_t *session,
                                           void *ctx);

http2_client_task_t *http2_client_task_connect(io_loop_t *loop,
                                               const char *host,
                                               const char *port,
                                               uint32_t stream_window_size,
                                               uint32_t connection_window_size);
http2_client_task_t *http2_client_task_new(io_loop_t *loop, int fd,
                                           uint32_t stream_window_size,
                                           uint32_t connection_window_size);
http2_client_session_t *http2_client_task_session(http2_client_task_t *t);
int http2_client_task_set_tls(http2_client_task_t *t,
                              tls_client_config_t *config, const char *host);
void http2_client_task_set_ready_cb(http2_client_task_t *t,
                                    http2_client_task_ready_cb cb, void *ctx);
void http2_client_task_set_close_cb(http2_client_task_t *t,
                                    http2_client_task_close_cb cb, void *ctx);
int http2_client_task_start(http2_client_task_t *t);
void http2_client_task_shutdown(http2_client_task_t *t);

#endif
