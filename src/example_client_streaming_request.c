/*
 * Example only; this file is not part of the Makefile build.
 *
 * HTTP/2 client streaming request sender over plain TCP (h2c prior knowledge).
 * It submits one application message and then END_STREAM. A real streaming
 * producer would keep the data provider alive, enqueue more messages, and finish
 * only when the RPC-level stream ends.
 *
 * TLS
 * ---
 * Replace the socket send/recv calls with SSL_write/SSL_read and perform the
 * TLS handshake with ALPN "h2" before creating the nghttp2 client session. The
 * request submission and response handling below do not change.
 *
 * Route
 * -----
 * The example targets the demo echo route /mw/echo. Message framing, buffering,
 * compression, and limits are server middleware concerns; HTTP/2 transports raw
 * DATA and stream lifecycle events.
 */

#include <arpa/inet.h>
#include <netdb.h>
#include <nghttp2/nghttp2.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

typedef struct {
  const uint8_t *data;
  size_t len;
  size_t off;
} stream_body_t;

typedef struct {
  int fd;
  int stream_done;
} stream_client_t;

static ssize_t stream_client_send(nghttp2_session *session,
                                  const uint8_t *data, size_t len, int flags,
                                  void *user_data) {
  (void)session;
  (void)flags;
  stream_client_t *c = user_data;
  size_t off = 0;
  while (off < len) {
    ssize_t n = send(c->fd, data + off, len - off, MSG_NOSIGNAL);
    if (n <= 0) {
      return NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    off += (size_t)n;
  }
  return (ssize_t)len;
}

static int stream_client_on_header(nghttp2_session *session,
                                   const nghttp2_frame *frame,
                                   const uint8_t *name, size_t namelen,
                                   const uint8_t *value, size_t valuelen,
                                   uint8_t flags, void *user_data) {
  (void)session;
  (void)frame;
  (void)flags;
  (void)user_data;
  if (namelen == 7 && memcmp(name, ":status", 7) == 0) {
    fprintf(stderr, "response status: %.*s\n", (int)valuelen, value);
  }
  return 0;
}

static int stream_client_on_data(nghttp2_session *session, uint8_t flags,
                                 int32_t stream_id, const uint8_t *data,
                                 size_t len, void *user_data) {
  (void)session;
  (void)flags;
  (void)stream_id;
  (void)user_data;
  fwrite(data, 1, len, stdout);
  return 0;
}

static int stream_client_on_close(nghttp2_session *session, int32_t stream_id,
                                  uint32_t error_code, void *user_data) {
  (void)session;
  (void)stream_id;
  (void)error_code;
  ((stream_client_t *)user_data)->stream_done = 1;
  return 0;
}

static ssize_t read_stream_body(nghttp2_session *session, int32_t stream_id,
                                uint8_t *buf, size_t length,
                                uint32_t *data_flags,
                                nghttp2_data_source *source,
                                void *user_data) {
  (void)session;
  (void)stream_id;
  (void)user_data;
  stream_body_t *b = source->ptr;
  size_t left = b->len - b->off;
  if (left == 0) {
    *data_flags |= NGHTTP2_DATA_FLAG_EOF;
    return 0;
  }
  size_t n = left < length ? left : length;
  memcpy(buf, b->data + b->off, n);
  b->off += n;
  if (b->off == b->len) {
    *data_flags |= NGHTTP2_DATA_FLAG_EOF;
  }
  return (ssize_t)n;
}

static int stream_connect_plain(const char *host, const char *port) {
  struct addrinfo hints;
  struct addrinfo *res = NULL;
  memset(&hints, 0, sizeof hints);
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  if (getaddrinfo(host, port, &hints, &res) != 0) {
    return -1;
  }
  int fd = -1;
  for (struct addrinfo *p = res; p != NULL; p = p->ai_next) {
    fd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
    if (fd < 0) {
      continue;
    }
    if (connect(fd, p->ai_addr, p->ai_addrlen) == 0) {
      break;
    }
    close(fd);
    fd = -1;
  }
  freeaddrinfo(res);
  return fd;
}

/*
 * Sends one application message as a streaming request. For a real producer,
 * keep the provider alive and return NGHTTP2_ERR_DEFERRED when no message is
 * ready, then call nghttp2_session_resume_data() when the app enqueues one.
 */
int example_client_stream_request_one(const char *host, const char *port,
                                      const char *path, const uint8_t *message,
                                      size_t message_len) {
  int fd = stream_connect_plain(host, port);
  if (fd < 0) {
    return -1;
  }

  stream_client_t client = {.fd = fd, .stream_done = 0};
  nghttp2_session_callbacks *cbs = NULL;
  nghttp2_session *session = NULL;
  if (nghttp2_session_callbacks_new(&cbs) != 0) {
    close(fd);
    return -1;
  }
  nghttp2_session_callbacks_set_send_callback(cbs, stream_client_send);
  nghttp2_session_callbacks_set_on_header_callback(cbs,
                                                   stream_client_on_header);
  nghttp2_session_callbacks_set_on_data_chunk_recv_callback(cbs,
                                                            stream_client_on_data);
  nghttp2_session_callbacks_set_on_stream_close_callback(cbs,
                                                         stream_client_on_close);
  if (nghttp2_session_client_new(&session, cbs, &client) != 0) {
    nghttp2_session_callbacks_del(cbs);
    close(fd);
    return -1;
  }
  nghttp2_submit_settings(session, NGHTTP2_FLAG_NONE, NULL, 0);

  char authority[256];
  snprintf(authority, sizeof authority, "%s:%s", host, port);
  nghttp2_nv headers[] = {
      {(uint8_t *)":method", (uint8_t *)"POST", 7, 4, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":scheme", (uint8_t *)"http", 7, 4, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":authority", (uint8_t *)authority, 10,
       (size_t)strlen(authority), NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":path", (uint8_t *)path, 5, strlen(path),
       NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)"content-type", (uint8_t *)"application/x-rpc", 12, 17,
       NGHTTP2_NV_FLAG_NONE},
  };

  stream_body_t request = {.data = message, .len = message_len, .off = 0};
  nghttp2_data_provider provider = {
      .source.ptr = &request,
      .read_callback = read_stream_body,
  };
  int32_t sid = nghttp2_submit_request(session, NULL, headers,
                                       sizeof headers / sizeof headers[0],
                                       &provider, NULL);
  if (sid < 0) {
    nghttp2_session_del(session);
    nghttp2_session_callbacks_del(cbs);
    close(fd);
    return -1;
  }

  uint8_t inbuf[16384];
  while (!client.stream_done) {
    if (nghttp2_session_send(session) != 0) {
      break;
    }
    if (!nghttp2_session_want_read(session)) {
      break;
    }
    ssize_t n = recv(fd, inbuf, sizeof inbuf, 0);
    if (n <= 0) {
      break;
    }
    if (nghttp2_session_mem_recv(session, inbuf, (size_t)n) < 0) {
      break;
    }
  }

  nghttp2_session_del(session);
  nghttp2_session_callbacks_del(cbs);
  close(fd);
  return client.stream_done ? 0 : -1;
}

int main(void) {
  signal(SIGPIPE, SIG_IGN);
  static const uint8_t message[] = "streaming request message";
  int rc = example_client_stream_request_one(
      "127.0.0.1", "8000", "/mw/echo", message, sizeof message - 1);
  return rc == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
