#include "http2_client.h"
#include "tls_client.h"
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>

struct http2_client_task {
  io_loop_t *loop;
  io_task_t *task;
  http2_client_session_t *h2;
  uint8_t *read_buffer;
  size_t read_size;
  uint8_t *plain_buffer;
  size_t plain_size;
  tls_client_config_t *tls_config;
  tls_client_conn_t *tls;
  const char *host;
  int tls_ready;
  int connecting;
  uint32_t stream_window;
  uint32_t connection_window;
  http2_client_task_close_cb close_cb;
  http2_client_task_ready_cb ready_cb;
  void *close_ctx;
  void *ready_ctx;
  int closed;
  int ending;
};

static ssize_t tls_task_write(void *ctx, const uint8_t *buf, size_t len) {
  return tls_client_conn_write(ctx, buf, len);
}

static void client_task_wake(void *ctx) {
  task_ready((io_task_t *)ctx);
}

static void client_task_attach_http2(http2_client_task_t *t) {
  if (t->h2 != NULL) {
    t->h2->wake = client_task_wake;
    t->h2->wake_ctx = t->task;
  }
}

static void client_task_finish(http2_client_task_t *t, int error) {
  if (t->closed) return;
  t->closed = 1;
  if (t->close_cb != NULL) t->close_cb(t->close_ctx, error);
}

static int client_task_feed_encrypted(http2_client_task_t *t, io_task_t *task) {
  if (task->bytes_buffered == 0) return 0;
  size_t buffered = task->bytes_buffered;
  size_t span = task_read_span(task);
  const uint8_t *head = task->read_buffer + task->read_head;
  if (tls_client_conn_feed(t->tls, head, span) != (ssize_t)span ||
      (span < buffered &&
       tls_client_conn_feed(t->tls, task->read_buffer, buffered - span) !=
           (ssize_t)(buffered - span))) {
    return -1;
  }
  task_read_consume(task, buffered);
  return 0;
}

static int client_task_feed_plain(http2_client_task_t *t) {
  for (;;) {
    ssize_t n = tls_client_conn_read(t->tls, t->plain_buffer, t->plain_size);
    if (n <= 0) return 0;
    if (http2_client_session_feed(t->h2, t->plain_buffer, (size_t)n) != 0)
      return -1;
  }
}

static void client_task_process(io_task_t *task) {
  http2_client_task_t *t = task->context;

  if (t->connecting) {
    int err = 0;
    socklen_t elen = sizeof err;
    getsockopt(task->fd, SOL_SOCKET, SO_ERROR, &err, &elen);
    if (err != 0) {
      client_task_finish(t, HTTP2_CLIENT_ERR_TRANSPORT);
      task->state = TASK_COMPLETED;
      return;
    }
    t->connecting = 0;
    if (t->tls_config != NULL) {
      t->tls = tls_client_conn_new(t->tls_config, task->fd, t->host);
      if (t->tls == NULL) {
        client_task_finish(t, HTTP2_CLIENT_ERR_MEMORY);
        task->state = TASK_COMPLETED;
        return;
      }
    } else {
      t->h2 = http2_client_session_new(task->fd, NULL, t->stream_window,
                                       t->connection_window);
      client_task_attach_http2(t);
      if (t->h2 == NULL) {
        client_task_finish(t, HTTP2_CLIENT_ERR_MEMORY);
        task->state = TASK_COMPLETED;
        return;
      }
      if (t->ready_cb != NULL) t->ready_cb(t, t->h2, t->ready_ctx);
    }
    task_start_read(task);
  }

  if (t->tls != NULL) {
    if (client_task_feed_encrypted(t, task) != 0) {
      t->ending = 1;
      fail_all_requests(t->h2, HTTP2_CLIENT_ERR_TRANSPORT);
      client_task_finish(t, HTTP2_CLIENT_ERR_TRANSPORT);
      task->state = TASK_COMPLETED;
      return;
    }
    if (!t->tls_ready) {
      int hs = tls_client_conn_handshake(t->tls);
      tls_client_conn_flush(t->tls);

      if (hs < 0) {
        client_task_finish(t, HTTP2_CLIENT_ERR_TRANSPORT);
        task->state = TASK_COMPLETED;
        return;
      }
      if (!hs) {
        task->state = tls_client_conn_pending_out(t->tls) != 0
                          ? TASK_WAIT_WRITE
                          : TASK_ACTIVE;
        return;
      }
      t->tls_ready = 1;
      http2_transport_t transport = {.ctx = t->tls, .write = tls_task_write};
      t->h2 = http2_client_session_new(task->fd, &transport, t->stream_window,
                                       t->connection_window);
      client_task_attach_http2(t);
      if (t->h2 == NULL) {
        client_task_finish(t, HTTP2_CLIENT_ERR_MEMORY);
        task->state = TASK_COMPLETED;
        return;
      }
      if (t->ready_cb != NULL) t->ready_cb(t, t->h2, t->ready_ctx);
    }
    if (client_task_feed_plain(t) != 0) {
      t->ending = 1;
      fail_all_requests(t->h2, HTTP2_CLIENT_ERR_TRANSPORT);
      client_task_finish(t, HTTP2_CLIENT_ERR_TRANSPORT);
      task->state = TASK_COMPLETED;
      return;
    }
  } else if (task->bytes_buffered > 0) {
    size_t buffered = task->bytes_buffered;
    size_t span = task_read_span(task);
    const uint8_t *head = task->read_buffer + task->read_head;
    if (http2_client_session_feed(t->h2, head, span) != 0 ||
        (span < buffered &&
         http2_client_session_feed(t->h2, task->read_buffer, buffered - span) !=
             0)) {
      t->ending = 1;
      fail_all_requests(t->h2, HTTP2_CLIENT_ERR_TRANSPORT);
      client_task_finish(t, HTTP2_CLIENT_ERR_TRANSPORT);
      task->state = TASK_COMPLETED;
      return;
    }
    task_read_consume(task, buffered);
  }

  if (t->h2 != NULL && http2_client_flush(t->h2) != 0) {
    t->ending = 1;
    fail_all_requests(t->h2, HTTP2_CLIENT_ERR_TRANSPORT);
    client_task_finish(t, HTTP2_CLIENT_ERR_TRANSPORT);
    task->state = TASK_COMPLETED;
    return;
  }
  if (t->tls != NULL) {
    tls_client_conn_flush(t->tls);
  }
  int want_write =
      (t->h2 != NULL && http2_client_session_want_write(t->h2)) ||
      (t->tls != NULL && tls_client_conn_pending_out(t->tls) != 0);
  if (task->read_closed) {
    task->state = want_write ? TASK_WAIT_WRITE : TASK_COMPLETED;
    return;
  }
  task->state = want_write ? TASK_WAIT_WRITE : TASK_ACTIVE;
}

static void client_task_complete(void *context, int status,
                                 uint8_t *read_buffer) {
  (void)status;
  (void)read_buffer;
  http2_client_task_t *t = context;
  if (t == NULL) return;
  t->ending = 1;
  if (t->h2 != NULL) fail_all_requests(t->h2, HTTP2_CLIENT_ERR_TRANSPORT);
  client_task_finish(t, HTTP2_CLIENT_OK);
  http2_client_session_free(t->h2);
  tls_client_conn_free(t->tls);
  free(t->read_buffer);
  free(t->plain_buffer);
  free(t);
}

static void client_task_cancel(void *context) {
  http2_client_task_t *t = context;
  if (t == NULL) return;
  t->ending = 1;
  if (t->tls != NULL && t->tls_ready) {
    tls_client_conn_shutdown(t->tls);
    tls_client_conn_flush(t->tls);
  }
  if (t->h2 != NULL) fail_all_requests(t->h2, HTTP2_CLIENT_ERR_ABORTED);
  client_task_finish(t, HTTP2_CLIENT_ERR_ABORTED);
  http2_client_session_free(t->h2);
  tls_client_conn_free(t->tls);
  free(t->read_buffer);
  free(t->plain_buffer);
  free(t);
}

http2_client_task_t *http2_client_task_connect(io_loop_t *loop,
                                               const char *host,
                                               const char *port,
                                               uint32_t stream_window_size,
                                               uint32_t connection_window_size) {
  if (loop == NULL || host == NULL || port == NULL) return NULL;
  struct addrinfo hints;
  struct addrinfo *res = NULL;
  memset(&hints, 0, sizeof hints);
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  if (getaddrinfo(host, port, &hints, &res) != 0) return NULL;
  int fd = -1;
  for (struct addrinfo *p = res; p != NULL; p = p->ai_next) {
    fd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
    if (fd < 0) continue;
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    int rc = connect(fd, p->ai_addr, p->ai_addrlen);
    if (rc == 0 || errno == EINPROGRESS) break;
    close(fd);
    fd = -1;
  }
  freeaddrinfo(res);
  if (fd < 0) return NULL;
  http2_client_task_t *t = http2_client_task_new(loop, fd, stream_window_size,
                                                 connection_window_size);
  if (t == NULL) {
    close(fd);
    return NULL;
  }
  t->connecting = 1;
  return t;
}

http2_client_task_t *http2_client_task_new(io_loop_t *loop, int fd,
                                           uint32_t stream_window_size,
                                           uint32_t connection_window_size) {
  if (loop == NULL || fd < 0) return NULL;
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0) return NULL;
  http2_client_task_t *t = calloc(1, sizeof *t);
  if (t == NULL) return NULL;
  t->loop = loop;
  t->read_size = 16384;
  t->plain_size = 16384;
  t->stream_window = stream_window_size;
  t->connection_window = connection_window_size;
  t->read_buffer = malloc(t->read_size);
  t->plain_buffer = malloc(t->plain_size);
  if (t->read_buffer == NULL || t->plain_buffer == NULL) {
    free(t->read_buffer);
    free(t->plain_buffer);
    free(t);
    return NULL;
  }
  t->task = task_create();
  if (t->task == NULL) {
    free(t->read_buffer);
    free(t->plain_buffer);
    free(t);
    return NULL;
  }
  t->task->fd = fd;
  t->task->context = t;
  t->task->process_handler = client_task_process;
  t->task->on_complete = client_task_complete;
  t->task->on_cancel = client_task_cancel;
  return t;
}

http2_client_session_t *http2_client_task_session(http2_client_task_t *t) {
  return t != NULL ? t->h2 : NULL;
}

void http2_client_task_set_close_cb(http2_client_task_t *t,
                                    http2_client_task_close_cb cb, void *ctx) {
  if (t == NULL) return;
  t->close_cb = cb;
  t->close_ctx = ctx;
}

int http2_client_task_set_tls(http2_client_task_t *t,
                                tls_client_config_t *config,
                                const char *host) {
  if (t == NULL || config == NULL || host == NULL) return -1;
  t->tls_config = config;
  t->host = host;
  return 0;
}

void http2_client_task_set_ready_cb(http2_client_task_t *t,
                                    http2_client_task_ready_cb cb, void *ctx) {
  if (t == NULL) return;
  t->ready_cb = cb;
  t->ready_ctx = ctx;
}

int http2_client_task_start(http2_client_task_t *t) {
  if (t == NULL) return -1;
  if (t->connecting) {
    if (task_setup_read(t->task, t->read_buffer, t->read_size) != 0) {
      return -1;
    }
    if (io_loop_add_task(t->loop, t->task) != 0) {
      return -1;
    }
    task_wait_write(t->task);
    return 0;
  }
  if (t->tls_config != NULL) {
    t->tls = tls_client_conn_new(t->tls_config, t->task->fd, t->host);
    if (t->tls == NULL) return -1;
  } else {
    t->h2 = http2_client_session_new(t->task->fd, NULL, t->stream_window,
                                     t->connection_window);
    client_task_attach_http2(t);
    if (t->h2 == NULL) return -1;
  }
  if (task_setup_read(t->task, t->read_buffer, t->read_size) != 0 ||
      io_loop_add_task(t->loop, t->task) != 0) {
    return -1;
  }
  if (task_start_read(t->task) != 0) {
    return -1;
  }
  if (t->tls == NULL && http2_client_flush(t->h2) != 0) {
    return -1;
  }
  task_ready(t->task);
  return 0;
}

void http2_client_task_shutdown(http2_client_task_t *t) {
  if (t == NULL || t->task == NULL) return;
  if (t->ending) return;
  task_cancel(t->task);
}
