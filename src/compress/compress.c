#include "../../include/compress.h"
#include <brotli/decode.h>
#include <brotli/encode.h>
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>
#include <zstd.h>

struct compress_ctx {
  compress_algorithm_t algorithm;
  int encoder;
  int done;
  size_t max_output;
  size_t total_out;
  union {
    z_stream z;
    ZSTD_CStream *zc;
    ZSTD_DStream *zd;
    BrotliEncoderState *be;
    BrotliDecoderState *bd;
  } u;
};

static int gzip_level(compress_profile_t profile) {
  switch (profile) {
  case COMPRESS_FAST:
    return 1;
  case COMPRESS_BEST:
    return 9;
  case COMPRESS_BALANCED:
  default:
    return 6;
  }
}

static int zstd_level(compress_profile_t profile) {
  switch (profile) {
  case COMPRESS_FAST:
    return 1;
  case COMPRESS_BEST:
    return 19;
  case COMPRESS_BALANCED:
  default:
    return 3;
  }
}

static int brotli_quality(compress_profile_t profile) {
  switch (profile) {
  case COMPRESS_FAST:
    return 1;
  case COMPRESS_BEST:
    return 11;
  case COMPRESS_BALANCED:
  default:
    return 5;
  }
}

static compress_ctx_t *compress_ctx_alloc(compress_algorithm_t algorithm,
                                          int encoder, size_t max_output) {
  compress_ctx_t *ctx = calloc(1, sizeof *ctx);
  if (ctx == NULL) {
    return NULL;
  }
  ctx->algorithm = algorithm;
  ctx->encoder = encoder;
  ctx->max_output = max_output;
  return ctx;
}

compress_ctx_t *compress_encoder_new(compress_algorithm_t algorithm,
                                     compress_profile_t profile) {
  compress_ctx_t *ctx = compress_ctx_alloc(algorithm, 1, 0);
  if (ctx == NULL) {
    return NULL;
  }
  switch (algorithm) {
  case COMPRESS_GZIP:
    if (deflateInit2(&ctx->u.z, gzip_level(profile), Z_DEFLATED, 15 + 16, 8,
                     Z_DEFAULT_STRATEGY) != Z_OK) {
      free(ctx);
      return NULL;
    }
    break;
  case COMPRESS_ZSTD:
    ctx->u.zc = ZSTD_createCStream();
    if (ctx->u.zc == NULL ||
        ZSTD_isError(ZSTD_initCStream(ctx->u.zc, zstd_level(profile)))) {
      if (ctx->u.zc != NULL) {
        ZSTD_freeCStream(ctx->u.zc);
      }
      free(ctx);
      return NULL;
    }
    break;
  case COMPRESS_BROTLI:
    ctx->u.be = BrotliEncoderCreateInstance(NULL, NULL, NULL);
    if (ctx->u.be == NULL ||
        !BrotliEncoderSetParameter(ctx->u.be, BROTLI_PARAM_QUALITY,
                                   (uint32_t)brotli_quality(profile))) {
      if (ctx->u.be != NULL) {
        BrotliEncoderDestroyInstance(ctx->u.be);
      }
      free(ctx);
      return NULL;
    }
    break;
  default:
    free(ctx);
    return NULL;
  }
  return ctx;
}

compress_ctx_t *compress_decoder_new(compress_algorithm_t algorithm,
                                     size_t max_output) {
  if (max_output == 0) {
    return NULL;
  }
  compress_ctx_t *ctx = compress_ctx_alloc(algorithm, 0, max_output);
  if (ctx == NULL) {
    return NULL;
  }
  switch (algorithm) {
  case COMPRESS_GZIP:
    if (inflateInit2(&ctx->u.z, 15 + 32) != Z_OK) {
      free(ctx);
      return NULL;
    }
    break;
  case COMPRESS_ZSTD:
    ctx->u.zd = ZSTD_createDStream();
    if (ctx->u.zd == NULL ||
        ZSTD_isError(ZSTD_initDStream(ctx->u.zd))) {
      if (ctx->u.zd != NULL) {
        ZSTD_freeDStream(ctx->u.zd);
      }
      free(ctx);
      return NULL;
    }
    break;
  case COMPRESS_BROTLI:
    ctx->u.bd = BrotliDecoderCreateInstance(NULL, NULL, NULL);
    if (ctx->u.bd == NULL) {
      free(ctx);
      return NULL;
    }
    break;
  default:
    free(ctx);
    return NULL;
  }
  return ctx;
}

void compress_ctx_free(compress_ctx_t *ctx) {
  if (ctx == NULL) {
    return;
  }
  switch (ctx->algorithm) {
  case COMPRESS_GZIP:
    if (ctx->encoder) {
      deflateEnd(&ctx->u.z);
    } else {
      inflateEnd(&ctx->u.z);
    }
    break;
  case COMPRESS_ZSTD:
    if (ctx->encoder) {
      ZSTD_freeCStream(ctx->u.zc);
    } else {
      ZSTD_freeDStream(ctx->u.zd);
    }
    break;
  case COMPRESS_BROTLI:
    if (ctx->encoder) {
      BrotliEncoderDestroyInstance(ctx->u.be);
    } else {
      BrotliDecoderDestroyInstance(ctx->u.bd);
    }
    break;
  }
  free(ctx);
}

static compress_status_t gzip_run(compress_ctx_t *ctx, const uint8_t *in,
                                  size_t in_len, size_t *in_used,
                                  uint8_t *out, size_t out_cap,
                                  size_t *out_used, int finish) {
  z_stream *z = &ctx->u.z;
  if (in_len > UINT_MAX) {
    in_len = UINT_MAX;
  }
  if (out_cap > UINT_MAX) {
    out_cap = UINT_MAX;
  }
  z->next_in = (Bytef *)in;
  z->avail_in = (uInt)in_len;
  z->next_out = out;
  z->avail_out = (uInt)out_cap;
  int flush = finish ? Z_FINISH : Z_NO_FLUSH;

  for (;;) {
    size_t before_in = z->avail_in;
    size_t before_out = z->avail_out;
    int rc = deflate(z, flush);
    if (rc == Z_STREAM_END) {
      ctx->done = 1;
      break;
    }
    if (rc != Z_OK && rc != Z_BUF_ERROR) {
      return COMPRESS_ERROR;
    }
    if (z->avail_out == 0) {
      break;
    }
    if (!finish && z->avail_in == 0) {
      break;
    }
    if (z->avail_in == before_in && z->avail_out == before_out) {
      break;
    }
  }
  *in_used = in_len - z->avail_in;
  *out_used = out_cap - z->avail_out;
  return ctx->done ? COMPRESS_DONE : COMPRESS_OK;
}

static compress_status_t zstd_encoder_run(compress_ctx_t *ctx,
                                          const uint8_t *in, size_t in_len,
                                          size_t *in_used, uint8_t *out,
                                          size_t out_cap, size_t *out_used,
                                          int finish) {
  ZSTD_inBuffer ib = {in, in_len, 0};
  ZSTD_outBuffer ob = {out, out_cap, 0};
  ZSTD_EndDirective end = finish ? ZSTD_e_end : ZSTD_e_continue;

  for (;;) {
    size_t before_in = ib.pos;
    size_t before_out = ob.pos;
    size_t rc = ZSTD_compressStream2(ctx->u.zc, &ob, &ib, end);
    if (ZSTD_isError(rc)) {
      return COMPRESS_ERROR;
    }
    if (finish && rc == 0) {
      ctx->done = 1;
      break;
    }
    if (ob.pos == ob.size) {
      break;
    }
    if (!finish && ib.pos == ib.size) {
      break;
    }
    if (ib.pos == before_in && ob.pos == before_out) {
      break;
    }
  }
  *in_used = ib.pos;
  *out_used = ob.pos;
  return ctx->done ? COMPRESS_DONE : COMPRESS_OK;
}

static compress_status_t brotli_encoder_run(compress_ctx_t *ctx,
                                            const uint8_t *in, size_t in_len,
                                            size_t *in_used, uint8_t *out,
                                            size_t out_cap, size_t *out_used,
                                            int finish) {
  const uint8_t *next_in = in;
  uint8_t *next_out = out;
  size_t avail_in = in_len;
  size_t avail_out = out_cap;
  BrotliEncoderOperation op =
      finish ? BROTLI_OPERATION_FINISH : BROTLI_OPERATION_PROCESS;

  for (;;) {
    size_t before_in = avail_in;
    size_t before_out = avail_out;
    if (!BrotliEncoderCompressStream(ctx->u.be, op, &avail_in, &next_in,
                                     &avail_out, &next_out, NULL)) {
      return COMPRESS_ERROR;
    }
    if (finish && BrotliEncoderIsFinished(ctx->u.be)) {
      ctx->done = 1;
      break;
    }
    if (avail_out == 0) {
      break;
    }
    if (!finish && avail_in == 0) {
      break;
    }
    if (avail_in == before_in && avail_out == before_out) {
      break;
    }
  }
  *in_used = in_len - avail_in;
  *out_used = out_cap - avail_out;
  return ctx->done ? COMPRESS_DONE : COMPRESS_OK;
}

static size_t decoder_out_cap(compress_ctx_t *ctx, size_t out_cap) {
  if (ctx->total_out >= ctx->max_output) {
    return 0;
  }
  size_t left = ctx->max_output - ctx->total_out;
  return out_cap < left ? out_cap : left;
}

static compress_status_t gzip_decoder_run(compress_ctx_t *ctx,
                                          const uint8_t *in, size_t in_len,
                                          size_t *in_used, uint8_t *out,
                                          size_t out_cap, size_t *out_used) {
  z_stream *z = &ctx->u.z;
  out_cap = decoder_out_cap(ctx, out_cap);
  if (out_cap == 0) {
    return COMPRESS_LIMIT;
  }
  if (in_len > UINT_MAX) {
    in_len = UINT_MAX;
  }
  if (out_cap > UINT_MAX) {
    out_cap = UINT_MAX;
  }
  z->next_in = (Bytef *)in;
  z->avail_in = (uInt)in_len;
  z->next_out = out;
  z->avail_out = (uInt)out_cap;

  for (;;) {
    size_t before_in = z->avail_in;
    size_t before_out = z->avail_out;
    int rc = inflate(z, Z_NO_FLUSH);
    if (rc == Z_STREAM_END) {
      ctx->done = 1;
      break;
    }
    if (rc != Z_OK && rc != Z_BUF_ERROR) {
      return COMPRESS_ERROR;
    }
    if (z->avail_out == 0 || z->avail_in == 0) {
      break;
    }
    if (z->avail_in == before_in && z->avail_out == before_out) {
      break;
    }
  }
  *in_used = in_len - z->avail_in;
  *out_used = out_cap - z->avail_out;
  ctx->total_out += *out_used;
  return ctx->done ? COMPRESS_DONE : COMPRESS_OK;
}

static compress_status_t zstd_decoder_run(compress_ctx_t *ctx,
                                          const uint8_t *in, size_t in_len,
                                          size_t *in_used, uint8_t *out,
                                          size_t out_cap, size_t *out_used) {
  out_cap = decoder_out_cap(ctx, out_cap);
  if (out_cap == 0) {
    return COMPRESS_LIMIT;
  }
  ZSTD_inBuffer ib = {in, in_len, 0};
  ZSTD_outBuffer ob = {out, out_cap, 0};

  for (;;) {
    size_t before_in = ib.pos;
    size_t before_out = ob.pos;
    size_t rc = ZSTD_decompressStream(ctx->u.zd, &ob, &ib);
    if (ZSTD_isError(rc)) {
      return COMPRESS_ERROR;
    }
    if (rc == 0) {
      ctx->done = 1;
      break;
    }
    if (ob.pos == ob.size || ib.pos == ib.size) {
      break;
    }
    if (ib.pos == before_in && ob.pos == before_out) {
      break;
    }
  }
  *in_used = ib.pos;
  *out_used = ob.pos;
  ctx->total_out += ob.pos;
  return ctx->done ? COMPRESS_DONE : COMPRESS_OK;
}

static compress_status_t brotli_decoder_run(compress_ctx_t *ctx,
                                            const uint8_t *in, size_t in_len,
                                            size_t *in_used, uint8_t *out,
                                            size_t out_cap, size_t *out_used) {
  out_cap = decoder_out_cap(ctx, out_cap);
  if (out_cap == 0) {
    return COMPRESS_LIMIT;
  }
  const uint8_t *next_in = in;
  uint8_t *next_out = out;
  size_t avail_in = in_len;
  size_t avail_out = out_cap;

  for (;;) {
    size_t before_in = avail_in;
    size_t before_out = avail_out;
    BrotliDecoderResult rc = BrotliDecoderDecompressStream(
        ctx->u.bd, &avail_in, &next_in, &avail_out, &next_out, NULL);
    if (rc == BROTLI_DECODER_RESULT_SUCCESS) {
      ctx->done = 1;
      break;
    }
    if (rc == BROTLI_DECODER_RESULT_ERROR) {
      return COMPRESS_ERROR;
    }
    if (rc == BROTLI_DECODER_RESULT_NEEDS_MORE_OUTPUT) {
      break;
    }
    if (rc == BROTLI_DECODER_RESULT_NEEDS_MORE_INPUT) {
      break;
    }
    if (avail_in == before_in && avail_out == before_out) {
      break;
    }
  }
  *in_used = in_len - avail_in;
  *out_used = out_cap - avail_out;
  ctx->total_out += *out_used;
  return ctx->done ? COMPRESS_DONE : COMPRESS_OK;
}

compress_status_t compress_ctx_run(compress_ctx_t *ctx, const uint8_t *in,
                                   size_t in_len, size_t *in_used,
                                   uint8_t *out, size_t out_cap,
                                   size_t *out_used, int finish) {
  size_t used_in = 0;
  size_t used_out = 0;
  if (in_used != NULL) {
    *in_used = 0;
  }
  if (out_used != NULL) {
    *out_used = 0;
  }
  if (ctx == NULL || out == NULL || out_cap == 0 || (in_len > 0 && in == NULL)) {
    return COMPRESS_ERROR;
  }
  if (ctx->done) {
    return COMPRESS_DONE;
  }

  compress_status_t status;
  if (ctx->encoder) {
    switch (ctx->algorithm) {
    case COMPRESS_GZIP:
      status = gzip_run(ctx, in, in_len, &used_in, out, out_cap, &used_out,
                        finish);
      break;
    case COMPRESS_ZSTD:
      status = zstd_encoder_run(ctx, in, in_len, &used_in, out, out_cap,
                                &used_out, finish);
      break;
    case COMPRESS_BROTLI:
      status = brotli_encoder_run(ctx, in, in_len, &used_in, out, out_cap,
                                  &used_out, finish);
      break;
    default:
      return COMPRESS_ERROR;
    }
  } else {
    switch (ctx->algorithm) {
    case COMPRESS_GZIP:
      status = gzip_decoder_run(ctx, in, in_len, &used_in, out, out_cap,
                                &used_out);
      break;
    case COMPRESS_ZSTD:
      status = zstd_decoder_run(ctx, in, in_len, &used_in, out, out_cap,
                                &used_out);
      break;
    case COMPRESS_BROTLI:
      status = brotli_decoder_run(ctx, in, in_len, &used_in, out, out_cap,
                                  &used_out);
      break;
    default:
      return COMPRESS_ERROR;
    }
  }
  if (in_used != NULL) {
    *in_used = used_in;
  }
  if (out_used != NULL) {
    *out_used = used_out;
  }
  return status;
}

const char *compress_algorithm_name(compress_algorithm_t algorithm) {
  switch (algorithm) {
  case COMPRESS_GZIP:
    return "gzip";
  case COMPRESS_ZSTD:
    return "zstd";
  case COMPRESS_BROTLI:
    return "brotli";
  default:
    return "unknown";
  }
}
