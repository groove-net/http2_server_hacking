#include "flow_control.h"
#include "http2_client.h"
#include "compress.h"
#include "liblog.h"
#include <errno.h>
#include <netdb.h>
#include <nghttp2/nghttp2.h>
#include <sys/socket.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/uio.h>
#include <unistd.h>

struct http2_client_request {
  http2_client_session_t *session;
  struct http2_client_request *next;
  int32_t sid;
  int is_stream;
  int done;
  int started;
  int aborted;
  int error;
  int status;
  http2_header_t *headers;
  http2_header_t *headers_tail;
  http2_client_request_opts_t opts;
  http2_client_unary_cb unary_cb;
  http2_client_stream_start_cb start_cb;
  http2_client_stream_msg_cb msg_cb;
  http2_client_stream_end_cb end_cb;
  void *ctx;
  uint8_t *send_body;
  size_t send_len;
  size_t send_off;
  uint8_t *raw;
  size_t raw_len;
  size_t raw_cap;
  compress_ctx_t *decoder;
  int decoder_done;
  uint8_t *residual;
  size_t residual_len;
  size_t residual_off;
  size_t residual_cap;
  size_t total_decoded;
  int req_stream;
  int hold_credit;
  size_t held_wire;
  flow_window_t flow;
  int flow_active;
  void *body_ctx;
  int req_finished;
  int req_eof;
  compress_ctx_t *req_encoder;
  uint8_t *req_out;
  size_t req_out_len;
  size_t req_out_off;
  size_t req_out_cap;
};

struct http2_client_session {
  nghttp2_session *session;
  int fd;
  http2_write_fn write_fn;
  void *transport_ctx;
  http2_client_request_t *requests;
  flow_policy_t flow_policy;
  flow_window_t conn_flow;
  uint32_t stream_window;
  uint32_t connection_window;
  void (*wake)(void *ctx);
  void *wake_ctx;
};

static http2_client_request_t *find_request(http2_client_session_t *s,
                                            int32_t sid) {
  for (http2_client_request_t *r = s->requests; r != NULL; r = r->next) {
    if (r->sid == sid) return r;
  }
  return NULL;
}

static ssize_t client_send_cb(nghttp2_session *session, const uint8_t *data,
                              size_t len, int flags, void *user_data) {
  (void)session;
  (void)flags;
  http2_client_session_t *s = user_data;
  if (s->write_fn != NULL) {
    return s->write_fn(s->transport_ctx, data, len);
  }
  size_t off = 0;
  while (off < len) {
    ssize_t n = write(s->fd, data + off, len - off);
    if (n > 0) {
      off += (size_t)n;
      continue;
    }
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      if (off == 0) return NGHTTP2_ERR_WOULDBLOCK;
      return (ssize_t)off;
    }
    return NGHTTP2_ERR_CALLBACK_FAILURE;
  }
  return (ssize_t)len;
}

static void free_headers(http2_header_t *h) {
  while (h != NULL) {
    http2_header_t *next = h->next;
    free(h->name);
    free(h);
    h = next;
  }
}

static int append_header(http2_client_request_t *r, const uint8_t *name,
                         size_t namelen, const uint8_t *value,
                         size_t valuelen) {
  http2_header_t *h = calloc(1, sizeof *h);
  if (h == NULL) return -1;
  uint8_t *blk = malloc(namelen + valuelen + 1);
  if (blk == NULL) {
    free(h);
    return -1;
  }
  memcpy(blk, name, namelen);
  memcpy(blk + namelen, value, valuelen);
  h->name = blk;
  h->namelen = namelen;
  h->value = blk + namelen;
  h->valuelen = valuelen;
  if (r->headers_tail != NULL) {
    r->headers_tail->next = h;
  } else {
    r->headers = h;
  }
  r->headers_tail = h;
  return 0;
}

static int client_on_header(nghttp2_session *session,
                            const nghttp2_frame *frame, const uint8_t *name,
                            size_t namelen, const uint8_t *value,
                            size_t valuelen, uint8_t flags, void *user_data) {
  (void)session;
  (void)flags;
  (void)user_data;
  http2_client_request_t *r =
      nghttp2_session_get_stream_user_data(session, frame->hd.stream_id);
  if (r == NULL) return 0;
  if (namelen == 7 && memcmp(name, ":status", 7) == 0 && valuelen == 3) {
    r->status = (value[0] - '0') * 100 + (value[1] - '0') * 10 + (value[2] - '0');
  }
  return append_header(r, name, namelen, value, valuelen);
}

static int append_raw(http2_client_request_t *r, const uint8_t *data,
                      size_t len) {
  size_t limit = r->opts.max_response_body != 0
                     ? r->opts.max_response_body
                     : HTTP2_MAX_BODY;
  if (r->raw_len > limit || len > limit - r->raw_len) return -1;
  if (r->raw_len + len > r->raw_cap) {
    size_t cap = r->raw_cap ? r->raw_cap : 1024;
    while (cap < r->raw_len + len) cap *= 2;
    uint8_t *p = realloc(r->raw, cap);
    if (p == NULL) return -1;
    r->raw = p;
    r->raw_cap = cap;
  }
  memcpy(r->raw + r->raw_len, data, len);
  r->raw_len += len;
  return 0;
}

static int response_stream_process(http2_client_request_t *r,
                                    const uint8_t *data, size_t len,
                                    int final_hint) {
  size_t cap = r->opts.max_response_body != 0 ? r->opts.max_response_body
                                              : HTTP2_MAX_BODY;
  if (r->total_decoded > cap || len > cap - r->total_decoded) {
    r->error = HTTP2_CLIENT_ERR_LIMIT;
    return -1;
  }
  size_t msg_cap = r->opts.max_response_message_size != 0
                       ? r->opts.max_response_message_size
                       : HTTP2_MAX_RESIDUAL;
  if (r->residual_cap == 0) {
    r->residual_cap = msg_cap + 16384;
    r->residual = malloc(r->residual_cap);
    if (r->residual == NULL) {
      r->error = HTTP2_CLIENT_ERR_MEMORY;
      return -1;
    }
  }
  if (r->residual_len > r->residual_cap || len > r->residual_cap - r->residual_len) {
    r->error = HTTP2_CLIENT_ERR_PROTOCOL;
    return -1;
  }
  memcpy(r->residual + r->residual_len, data, len);
  r->residual_len += len;
  r->total_decoded += len;

  size_t cur = r->residual_off;
  while (cur < r->residual_len) {
    http2_view_t v = {r->residual, r->residual_len, NULL, 0};
    size_t m = r->opts.response_framing(r->opts.response_framing_cfg, &v, cur,
                                        r->residual_len - cur);
    if (m == 0) break;
    if (m > msg_cap) {
      r->error = HTTP2_CLIENT_ERR_PROTOCOL;
      return -1;
    }
    if (r->msg_cb != NULL) {
      int last = final_hint && (cur + m == r->residual_len);
      r->msg_cb(r->ctx, r->residual + cur, m, last);
    }
    cur += m;
  }
  r->residual_off = cur;
  if (cur > 0) {
    size_t left = r->residual_len - cur;
    if (left > 0) memmove(r->residual, r->residual + cur, left);
    r->residual_len = left;
    r->residual_off = 0;
  }
  return 0;
}

static void client_flow_received(http2_client_session_t *s,
                                 http2_client_request_t *r, size_t n) {
  flow_window_on_received(&s->conn_flow, (uint32_t)n);
  if (r->flow_active) flow_window_on_received(&r->flow, (uint32_t)n);
}

static int client_flow_consume(http2_client_session_t *s,
                               http2_client_request_t *r, size_t n) {
  flow_window_on_consumed(&s->conn_flow, (uint32_t)n);
  flow_credit_t cc = flow_window_update(&s->conn_flow, 0);
  if (cc.mode == FLOW_CREDIT_EXACT &&
      nghttp2_session_consume_connection(s->session, cc.bytes) != 0) {
    return -1;
  }
  if (r->flow_active && !r->done) {
    flow_window_on_consumed(&r->flow, (uint32_t)n);
    flow_credit_t cs = flow_window_update(&r->flow, 0);
    if (cs.mode == FLOW_CREDIT_EXACT &&
        nghttp2_session_consume_stream(s->session, r->sid, cs.bytes) != 0) {
      return -1;
    }
  }
  return 0;
}

static int client_on_data(nghttp2_session *session, uint8_t flags,
                          int32_t stream_id, const uint8_t *data, size_t len,
                          void *user_data) {
  (void)flags;
  (void)user_data;
  http2_client_request_t *r =
      nghttp2_session_get_stream_user_data(session, stream_id);
  if (r == NULL || r->aborted) return 0;
  client_flow_received(r->session, r, len);
  if (!r->is_stream) {
    if (append_raw(r, data, len) != 0) {
      r->error = HTTP2_CLIENT_ERR_LIMIT;
      return NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    if (r->hold_credit) {
      r->held_wire += len;
    } else if (client_flow_consume(r->session, r, len) != 0) {
      return NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    return 0;
  }

  if (r->decoder != NULL) {
    size_t off = 0;
    while (off < len) {
      uint8_t plain[16384];
      size_t iu = 0, ou = 0;
      compress_status_t cs =
          compress_ctx_run(r->decoder, data + off, len - off, &iu, plain,
                           sizeof plain, &ou, 0);
      off += iu;
      if (cs == COMPRESS_ERROR || cs == COMPRESS_LIMIT) {
        r->error = HTTP2_CLIENT_ERR_LIMIT;
        return NGHTTP2_ERR_CALLBACK_FAILURE;
      }
      if (ou > 0 &&
          response_stream_process(r, plain, ou, cs == COMPRESS_DONE) != 0) {
        return NGHTTP2_ERR_CALLBACK_FAILURE;
      }
      if (cs == COMPRESS_DONE) {
        r->decoder_done = 1;
        break;
      }
      if (iu == 0 && ou == 0) break;
    }
  } else {
    if (response_stream_process(r, data, len, 0) != 0) {
      return NGHTTP2_ERR_CALLBACK_FAILURE;
    }
  }
  if (r->hold_credit) {
    r->held_wire += len;
  } else if (client_flow_consume(r->session, r, len) != 0) {
    return NGHTTP2_ERR_CALLBACK_FAILURE;
  }
  return 0;
}

static int client_on_frame(nghttp2_session *session,
                           const nghttp2_frame *frame, void *user_data) {
  (void)user_data;
  if (frame->hd.type != NGHTTP2_HEADERS ||
      frame->headers.cat != NGHTTP2_HCAT_RESPONSE) {
    return 0;
  }
  http2_client_request_t *r =
      nghttp2_session_get_stream_user_data(session, frame->hd.stream_id);
  if (r == NULL || r->started || !r->is_stream) return 0;
  r->started = 1;
  http2_client_response_t resp = {r->status, r->headers};
  if (r->start_cb != NULL && r->start_cb(r->ctx, &resp, 0) != 0) {
    r->aborted = 1;
    nghttp2_submit_rst_stream(session, NGHTTP2_FLAG_NONE, r->sid,
                              NGHTTP2_CANCEL);
  }
  return 0;
}

static int decode_all(http2_compression_t compression,
                      size_t profile_unused, const uint8_t *in, size_t in_len,
                      size_t max_out, uint8_t **out, size_t *out_len) {
  (void)profile_unused;
  if (compression == HTTP2_COMPRESSION_OFF) {
    uint8_t *p = malloc(in_len ? in_len : 1);
    if (p == NULL) return -1;
    memcpy(p, in, in_len);
    *out = p;
    *out_len = in_len;
    return 0;
  }
  compress_algorithm_t alg = COMPRESS_GZIP;
  if (compression == HTTP2_COMPRESSION_ZSTD) alg = COMPRESS_ZSTD;
  if (compression == HTTP2_COMPRESSION_BROTLI) alg = COMPRESS_BROTLI;
  compress_ctx_t *c = compress_decoder_new(alg, max_out);
  if (c == NULL) return -1;
  size_t cap = 1024, len = 0;
  uint8_t *p = malloc(cap);
  if (p == NULL) {
    compress_ctx_free(c);
    return -1;
  }
  size_t off = 0;
  for (;;) {
    uint8_t buf[16384];
    size_t iu = 0, ou = 0;
    compress_status_t st =
        compress_ctx_run(c, in + off, in_len - off, &iu, buf, sizeof buf, &ou, 0);
    off += iu;
    if (len + ou > cap) {
      while (len + ou > cap) cap *= 2;
      uint8_t *q = realloc(p, cap);
      if (q == NULL) {
        free(p);
        compress_ctx_free(c);
        return -1;
      }
      p = q;
    }
    memcpy(p + len, buf, ou);
    len += ou;
    if (st == COMPRESS_DONE) break;
    if (st == COMPRESS_ERROR || st == COMPRESS_LIMIT) {
      free(p);
      compress_ctx_free(c);
      return -1;
    }
    if (iu == 0 && ou == 0) break;
  }
  compress_ctx_free(c);
  *out = p;
  *out_len = len;
  return 0;
}

static int encode_all(http2_compression_t compression,
                      http2_compression_profile_t profile, const uint8_t *in,
                      size_t in_len, uint8_t **out, size_t *out_len) {
  if (compression == HTTP2_COMPRESSION_OFF) {
    uint8_t *p = malloc(in_len ? in_len : 1);
    if (p == NULL) return -1;
    memcpy(p, in, in_len);
    *out = p;
    *out_len = in_len;
    return 0;
  }
  compress_algorithm_t alg = COMPRESS_GZIP;
  if (compression == HTTP2_COMPRESSION_ZSTD) alg = COMPRESS_ZSTD;
  if (compression == HTTP2_COMPRESSION_BROTLI) alg = COMPRESS_BROTLI;
  compress_profile_t cp = COMPRESS_FAST;
  if (profile == HTTP2_COMPRESSION_BALANCED) cp = COMPRESS_BALANCED;
  if (profile == HTTP2_COMPRESSION_BEST) cp = COMPRESS_BEST;
  compress_ctx_t *c = compress_encoder_new(alg, cp);
  if (c == NULL) return -1;
  size_t cap = in_len + 1024, len = 0;
  uint8_t *p = malloc(cap);
  if (p == NULL) {
    compress_ctx_free(c);
    return -1;
  }
  size_t off = 0;
  for (;;) {
    uint8_t buf[16384];
    size_t iu = 0, ou = 0;
    int finish = off == in_len;
    compress_status_t st = compress_ctx_run(c, in + off, in_len - off, &iu, buf,
                                            sizeof buf, &ou, finish);
    off += iu;
    if (len + ou > cap) {
      while (len + ou > cap) cap *= 2;
      uint8_t *q = realloc(p, cap);
      if (q == NULL) {
        free(p);
        compress_ctx_free(c);
        return -1;
      }
      p = q;
    }
    memcpy(p + len, buf, ou);
    len += ou;
    if (st == COMPRESS_DONE) break;
    if (st == COMPRESS_ERROR || st == COMPRESS_LIMIT) {
      free(p);
      compress_ctx_free(c);
      return -1;
    }
    if (iu == 0 && ou == 0 && finish) break;
  }
  compress_ctx_free(c);
  *out = p;
  *out_len = len;
  return 0;
}

static void finish_request(http2_client_request_t *r, int error) {
  if (r->done) return;
  r->done = 1;
  http2_client_response_t resp = {r->status, r->headers};
  if (r->aborted) error = HTTP2_CLIENT_ERR_ABORTED;
  if (!r->is_stream) {
    uint8_t *body = NULL;
    size_t len = 0;
    int rc = error == 0 ? decode_all(r->opts.compression, 0, r->raw, r->raw_len,
                                     r->opts.max_response_body ? r->opts.max_response_body
                                                               : HTTP2_MAX_BODY,
                                     &body, &len)
                        : -1;
    if (rc != 0) error = HTTP2_CLIENT_ERR_LIMIT;
    if (r->unary_cb != NULL)
      r->unary_cb(r->ctx, &resp, body, len, error);
    free(body);
    return;
  }

  if (error == 0 && !r->aborted) {
    if (r->opts.response_framing == NULL) {
      error = HTTP2_CLIENT_ERR_PROTOCOL;
    } else if (r->decoder != NULL && !r->decoder_done) {
      error = HTTP2_CLIENT_ERR_PROTOCOL;
    } else if (r->residual_len != 0) {
      error = HTTP2_CLIENT_ERR_PROTOCOL;
    }
  }
  if (r->end_cb != NULL) r->end_cb(r->ctx, &resp, error);
}

static void fail_all_requests(http2_client_session_t *s, int error) {
  if (s == NULL) return;
  http2_client_request_t *r = s->requests;
  s->requests = NULL;
  while (r != NULL) {
    http2_client_request_t *next = r->next;
    r->next = NULL;
    if (!r->done) finish_request(r, error);
    r = next;
  }
}

static int client_on_close(nghttp2_session *session, int32_t stream_id,
                           uint32_t error_code, void *user_data) {
  http2_client_session_t *s = user_data;
  http2_client_request_t *r =
      nghttp2_session_get_stream_user_data(session, stream_id);
  if (r == NULL) r = find_request(s, stream_id);
  if (r == NULL) return 0;
  finish_request(r, error_code == 0 ? 0 : HTTP2_CLIENT_ERR_PROTOCOL);
  return 0;
}

http2_client_session_t *http2_client_session_new(
    int fd, const http2_transport_t *transport, uint32_t stream_window_size,
    uint32_t connection_window_size) {
  if (fd < 0 || stream_window_size == 0 || connection_window_size == 0)
    return NULL;
  http2_client_session_t *s = calloc(1, sizeof *s);
  if (s == NULL) return NULL;
  s->fd = fd;
  if (transport != NULL) {
    s->write_fn = transport->write;
    s->transport_ctx = transport->ctx;
  }
  nghttp2_session_callbacks *cbs = NULL;
  if (nghttp2_session_callbacks_new(&cbs) != 0) {
    free(s);
    return NULL;
  }
  nghttp2_session_callbacks_set_send_callback(cbs, client_send_cb);
  nghttp2_session_callbacks_set_on_header_callback(cbs, client_on_header);
  nghttp2_session_callbacks_set_on_data_chunk_recv_callback(cbs, client_on_data);
  nghttp2_session_callbacks_set_on_frame_recv_callback(cbs, client_on_frame);
  nghttp2_session_callbacks_set_on_stream_close_callback(cbs, client_on_close);
  nghttp2_option *opt = NULL;
  if (nghttp2_option_new(&opt) != 0) {
    nghttp2_session_callbacks_del(cbs);
    free(s);
    return NULL;
  }
  nghttp2_option_set_no_auto_window_update(opt, 1);
  if (nghttp2_session_client_new2(&s->session, cbs, s, opt) != 0) {
    nghttp2_option_del(opt);
    nghttp2_session_callbacks_del(cbs);
    free(s);
    return NULL;
  }
  nghttp2_option_del(opt);
  nghttp2_session_callbacks_del(cbs);
  nghttp2_session_set_local_window_size(s->session, NGHTTP2_FLAG_NONE, 0,
                                        (int32_t)connection_window_size);
  const nghttp2_settings_entry sv[] = {
      {NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE, stream_window_size},
  };
  nghttp2_submit_settings(s->session, NGHTTP2_FLAG_NONE, sv,
                          sizeof sv / sizeof sv[0]);
  s->stream_window = stream_window_size;
  s->connection_window = connection_window_size;
  flow_policy_profile(FLOW_PROFILE_DEFAULT, stream_window_size,
                      connection_window_size, &s->flow_policy);
  flow_window_init(&s->conn_flow, &s->flow_policy, connection_window_size);
  return s;
}

void http2_client_session_free(http2_client_session_t *s) {
  if (s == NULL) return;
  http2_client_request_t *r = s->requests;
  while (r != NULL) {
    http2_client_request_t *next = r->next;
    if (r->held_wire != 0) {
      nghttp2_session_consume_connection(s->session, r->held_wire);
    }
    free_headers(r->headers);
    compress_ctx_free(r->decoder);
    compress_ctx_free(r->req_encoder);
    free(r->send_body);
    free(r->raw);
    free(r->residual);
    free(r->req_out);
    free(r);
    r = next;
  }
  nghttp2_session_del(s->session);
  free(s);
}

int http2_client_session_feed(http2_client_session_t *s, const uint8_t *data,
                              size_t len) {
  if (s == NULL) return -1;
  nghttp2_ssize n = nghttp2_session_mem_recv2(s->session, data, len);
  if (n < 0) return -1;
  return 0;
}

int http2_client_flush(http2_client_session_t *s) {
  if (s == NULL) return -1;
  int rc = nghttp2_session_send(s->session);
  return rc == 0 || rc == NGHTTP2_ERR_WOULDBLOCK ? 0 : -1;
}

int http2_client_session_want_write(const http2_client_session_t *s) {
  return s != NULL && nghttp2_session_want_write(s->session) != 0;
}

int http2_client_session_want_read(const http2_client_session_t *s) {
  return s != NULL && nghttp2_session_want_read(s->session) != 0;
}

static int req_out_append(http2_client_request_t *r, const uint8_t *d, size_t n) {
  if (n == 0) return 0;
  if (r->req_out_len > r->req_out_cap || n > r->req_out_cap - r->req_out_len) {
    size_t cap = r->req_out_cap ? r->req_out_cap : 1024;
    while (cap < r->req_out_len + n) cap *= 2;
    uint8_t *p = realloc(r->req_out, cap);
    if (p == NULL) return -1;
    r->req_out = p;
    r->req_out_cap = cap;
  }
  memcpy(r->req_out + r->req_out_len, d, n);
  r->req_out_len += n;
  return 0;
}

static int client_request_fill(http2_client_request_t *r) {
  if (r->req_out_off < r->req_out_len) return 0;
  r->req_out_off = r->req_out_len = 0;
  if (r->req_finished) return 1;

  uint8_t plain[16384];
  for (;;) {
    if (r->req_eof) {
      if (r->req_encoder != NULL) {
        uint8_t out[16384];
        for (;;) {
          size_t iu = 0, ou = 0;
          compress_status_t cs =
              compress_ctx_run(r->req_encoder, NULL, 0, &iu, out, sizeof out,
                               &ou, 1);
          if (cs == COMPRESS_ERROR || cs == COMPRESS_LIMIT) return -1;
          if (ou > 0 && req_out_append(r, out, ou) != 0) return -1;
          if (cs == COMPRESS_DONE) {
            r->req_finished = 1;
            return r->req_out_len > 0 ? 0 : 1;
          }
          if (iu == 0 && ou == 0) return -1;
        }
      }
      r->req_finished = 1;
      return 1;
    }

    int eof = 0;
    ssize_t n = r->opts.body_read(r->body_ctx, plain, sizeof plain, &eof);
    if (n < 0) return -1;
    if (n == 0) {
      if (eof) {
        r->req_eof = 1;
        continue;
      }
      return -2;
    }
    if (eof) r->req_eof = 1;

    if (r->req_encoder != NULL) {
      uint8_t out[16384];
      size_t iu = 0, ou = 0;
      compress_status_t cs =
          compress_ctx_run(r->req_encoder, plain, (size_t)n, &iu, out,
                           sizeof out, &ou, r->req_eof ? 1 : 0);
      if (cs == COMPRESS_ERROR || cs == COMPRESS_LIMIT) return -1;
      if (ou > 0 && req_out_append(r, out, ou) != 0) return -1;
      if (ou > 0) return 0;
      if (r->req_eof) continue;
      if (iu == 0) return -1;
      continue;
    }

    if (req_out_append(r, plain, (size_t)n) != 0) return -1;
    return 0;
  }
}

static ssize_t read_send_body(nghttp2_session *session, int32_t stream_id,
                              uint8_t *buf, size_t length,
                              uint32_t *data_flags,
                              nghttp2_data_source *source, void *user_data) {
  (void)session;
  (void)stream_id;
  (void)user_data;
  http2_client_request_t *r = source->ptr;
  if (r->req_stream) {
    for (;;) {
      if (r->req_out_off < r->req_out_len) {
        size_t left = r->req_out_len - r->req_out_off;
        size_t n = left < length ? left : length;
        memcpy(buf, r->req_out + r->req_out_off, n);
        r->req_out_off += n;
        if (r->req_out_off == r->req_out_len && r->req_finished) {
          *data_flags |= NGHTTP2_DATA_FLAG_EOF;
        }
        return (ssize_t)n;
      }
      int st = client_request_fill(r);
      if (st == -2) return NGHTTP2_ERR_DEFERRED;
      if (st < 0) return NGHTTP2_ERR_CALLBACK_FAILURE;
      if (st == 1) {
        *data_flags |= NGHTTP2_DATA_FLAG_EOF;
        return 0;
      }
    }
  }
  size_t left = r->send_len - r->send_off;
  if (left == 0) {
    *data_flags |= NGHTTP2_DATA_FLAG_EOF;
    return 0;
  }
  size_t n = left < length ? left : length;
  memcpy(buf, r->send_body + r->send_off, n);
  r->send_off += n;
  if (r->send_off == r->send_len) *data_flags |= NGHTTP2_DATA_FLAG_EOF;
  return (ssize_t)n;
}

static http2_client_request_t *submit(
    http2_client_session_t *s, const http2_client_request_opts_t *opts,
    int is_stream, http2_client_unary_cb unary_cb,
    http2_client_stream_start_cb start_cb, http2_client_stream_msg_cb msg_cb,
    http2_client_stream_end_cb end_cb, void *ctx) {
  if (s == NULL || opts == NULL || opts->method == NULL || opts->path == NULL)
    return NULL;
  http2_client_request_t *r = calloc(1, sizeof *r);
  if (r == NULL) return NULL;
  r->session = s;
  r->is_stream = is_stream;
  r->opts = *opts;
  r->unary_cb = unary_cb;
  r->start_cb = start_cb;
  r->msg_cb = msg_cb;
  r->end_cb = end_cb;
  r->ctx = ctx;
  if (encode_all(opts->compression, opts->compression_profile, opts->body,
                 opts->body_len, &r->send_body, &r->send_len) != 0) {
    free(r);
    return NULL;
  }
  if (is_stream && opts->body_read != NULL) {
    r->req_stream = 1;
    r->body_ctx = opts->body_ctx;
    if (opts->compression != HTTP2_COMPRESSION_OFF) {
      compress_algorithm_t alg = COMPRESS_GZIP;
      if (opts->compression == HTTP2_COMPRESSION_ZSTD) alg = COMPRESS_ZSTD;
      if (opts->compression == HTTP2_COMPRESSION_BROTLI) alg = COMPRESS_BROTLI;
      compress_profile_t cp = COMPRESS_FAST;
      if (opts->compression_profile == HTTP2_COMPRESSION_BALANCED) cp = COMPRESS_BALANCED;
      if (opts->compression_profile == HTTP2_COMPRESSION_BEST) cp = COMPRESS_BEST;
      r->req_encoder = compress_encoder_new(alg, cp);
      if (r->req_encoder == NULL) {
        free(r->send_body);
        free(r);
        return NULL;
      }
    }
  }
  if (is_stream && opts->compression != HTTP2_COMPRESSION_OFF) {
    compress_algorithm_t alg = COMPRESS_GZIP;
    if (opts->compression == HTTP2_COMPRESSION_ZSTD) alg = COMPRESS_ZSTD;
    if (opts->compression == HTTP2_COMPRESSION_BROTLI) alg = COMPRESS_BROTLI;
    size_t max_out = opts->max_response_body != 0 ? opts->max_response_body
                                                  : HTTP2_MAX_BODY;
    r->decoder = compress_decoder_new(alg, max_out);
    if (r->decoder == NULL) {
      free(r->send_body);
      free(r);
      return NULL;
    }
  }

  nghttp2_nv *nva = calloc(opts->nheaders + 4, sizeof *nva);
  if (nva == NULL) {
    free(r->send_body);
    free(r);
    return NULL;
  }
  nva[0] = (nghttp2_nv){(uint8_t *)":method", (uint8_t *)opts->method, 7,
                        strlen(opts->method), NGHTTP2_NV_FLAG_NONE};
  nva[1] = (nghttp2_nv){(uint8_t *)":scheme", (uint8_t *)"http", 7, 4,
                        NGHTTP2_NV_FLAG_NONE};
  nva[2] = (nghttp2_nv){(uint8_t *)":authority",
                        (uint8_t *)(opts->authority ? opts->authority
                                                    : "localhost"),
                        10, strlen(opts->authority ? opts->authority
                                                   : "localhost"),
                        NGHTTP2_NV_FLAG_NONE};
  nva[3] = (nghttp2_nv){(uint8_t *)":path", (uint8_t *)opts->path, 5,
                        strlen(opts->path), NGHTTP2_NV_FLAG_NONE};
  for (size_t i = 0; i < opts->nheaders; i++) {
    nva[i + 4] = (nghttp2_nv){opts->headers[i].name, opts->headers[i].value,
                              opts->headers[i].namelen,
                              opts->headers[i].valuelen, NGHTTP2_NV_FLAG_NONE};
  }
  nghttp2_data_provider provider = {.source.ptr = r,
                                    .read_callback = read_send_body};
  int32_t sid =
      nghttp2_submit_request(s->session, NULL, nva, opts->nheaders + 4,
                             &provider, r);
  free(nva);
  if (sid < 0) {
    free(r->send_body);
    free(r);
    return NULL;
  }
  r->sid = sid;
  if (flow_window_init(&r->flow, &s->flow_policy, s->stream_window) == 0)
    r->flow_active = 1;
  nghttp2_session_set_local_window_size(s->session, NGHTTP2_FLAG_NONE, sid,
                                        (int32_t)s->stream_window);
  r->next = s->requests;
  s->requests = r;
  return r;
}

http2_client_request_t *http2_client_request_unary(
    http2_client_session_t *s, const http2_client_request_opts_t *opts,
    http2_client_unary_cb cb, void *ctx) {
  return submit(s, opts, 0, cb, NULL, NULL, NULL, ctx);
}

http2_client_request_t *http2_client_request_stream(
    http2_client_session_t *s, const http2_client_request_opts_t *opts,
    http2_client_stream_start_cb start_cb,
    http2_client_stream_msg_cb msg_cb, http2_client_stream_end_cb end_cb,
    void *ctx) {
  return submit(s, opts, 1, NULL, start_cb, msg_cb, end_cb, ctx);
}

int http2_client_request_hold(http2_client_request_t *req) {
  if (req == NULL) return -1;
  req->hold_credit = 1;
  return 0;
}

int http2_client_request_consume(http2_client_request_t *req, size_t n) {
  if (req == NULL) return -1;
  if (n == SIZE_MAX || n > req->held_wire) n = req->held_wire;
  if (n == 0) return 0;
  if (client_flow_consume(req->session, req, n) != 0) return -1;
  req->held_wire -= n;
  return 0;
}

int http2_client_request_resume(http2_client_request_t *req) {
  if (req == NULL || req->done) return -1;
  int rc = nghttp2_session_resume_data(req->session->session, req->sid);
  if (rc != 0) return -1;
  if (req->session->wake != NULL) req->session->wake(req->session->wake_ctx);
  return 0;
}

void http2_client_request_free(http2_client_request_t *req) {
  if (req == NULL) return;
  http2_client_session_t *s = req->session;
  if (!req->done && req->sid > 0) {
    nghttp2_submit_rst_stream(s->session, NGHTTP2_FLAG_NONE, req->sid,
                              NGHTTP2_CANCEL);
    nghttp2_session_set_stream_user_data(s->session, req->sid, NULL);
  }
  http2_client_request_t **link = &s->requests;
  while (*link != NULL && *link != req) link = &(*link)->next;
  if (*link == req) *link = req->next;
  if (req->held_wire != 0) {
    nghttp2_session_consume_connection(req->session->session, req->held_wire);
  }
  free_headers(req->headers);
  compress_ctx_free(req->decoder);
  compress_ctx_free(req->req_encoder);
  free(req->send_body);
  free(req->raw);
  free(req->residual);
  free(req->req_out);
  free(req);
}

#include "http2_client_task.c"
