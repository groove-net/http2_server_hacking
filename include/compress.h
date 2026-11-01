#ifndef COMPRESS_H
#define COMPRESS_H

/* Streaming compression/decompression for gzip, zstd, and brotli.
 *
 * The API is deliberately transport-neutral: it does not know about HTTP,
 * HTTP/2, gRPC, files, or sockets. One context is either an encoder or a
 * decoder. Feed input and drain output through caller-owned buffers:
 *
 *   while (...) {
 *     status = compress_ctx_run(ctx, in, in_len, &in_used,
 *                               out, out_cap, &out_used, finish);
 *     consume in_used input bytes and out_used output bytes;
 *     if (status == COMPRESS_DONE) break;
 *     if (status == COMPRESS_ERROR || status == COMPRESS_LIMIT) fail;
 *   }
 *
 * The caller must retry with any unconsumed input and a fresh output buffer.
 * Encoder calls use finish=0 for normal streaming and finish=1 for the final
 * calls; callers may need several finish calls to drain all output. Decoder
 * calls ignore finish and return COMPRESS_DONE when the compressed stream ends.
 */

#include <stddef.h>
#include <stdint.h>

typedef enum {
  COMPRESS_GZIP = 0, /* RFC 1952 gzip wrapper around DEFLATE */
  COMPRESS_ZSTD,     /* Zstandard frame */
  COMPRESS_BROTLI,   /* Brotli stream */
} compress_algorithm_t;

typedef enum {
  COMPRESS_FAST = 0, /* lowest CPU latency */
  COMPRESS_BALANCED, /* default tradeoff */
  COMPRESS_BEST,     /* smallest output, highest CPU */
} compress_profile_t;

typedef enum {
  COMPRESS_OK = 0, /* progress made; more input/output may be needed */
  COMPRESS_DONE,   /* end of stream reached */
  COMPRESS_ERROR,  /* invalid stream, library failure, or bad arguments */
  COMPRESS_LIMIT,  /* decoder exceeded its configured output limit */
} compress_status_t;

typedef struct compress_ctx compress_ctx_t;

/* Create an encoder. The context owns its algorithm state. Returns NULL for an
 * unsupported algorithm/profile or allocation failure. */
compress_ctx_t *compress_encoder_new(compress_algorithm_t algorithm,
                                     compress_profile_t profile);

/* Create a decoder. max_output is a hard total-output cap and must be nonzero;
 * it protects against decompression bombs. Returns NULL on bad arguments or
 * allocation failure. */
compress_ctx_t *compress_decoder_new(compress_algorithm_t algorithm,
                                     size_t max_output);

void compress_ctx_free(compress_ctx_t *ctx);

/* Run one bounded step.
 *
 * in/in_len is the remaining compressed or plaintext input. out/out_cap is the
 * output buffer. in_used and out_used receive progress (both are always set;
 * either pointer may be NULL). out_cap must be nonzero.
 *
 * For an encoder, finish=0 means "continue" and finish=1 means "finish the
 * stream". For a decoder, finish is ignored and COMPRESS_DONE means the
 * compressed end marker was reached; any unconsumed input is left for the
 * caller to inspect.
 */
compress_status_t compress_ctx_run(compress_ctx_t *ctx, const uint8_t *in,
                                   size_t in_len, size_t *in_used,
                                   uint8_t *out, size_t out_cap,
                                   size_t *out_used, int finish);

const char *compress_algorithm_name(compress_algorithm_t algorithm);

#endif
