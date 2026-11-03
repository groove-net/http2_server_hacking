# Compression library

This document describes the new three-in-one compression module:

- `include/compress.h`
- `src/compress/compress.c`

It is a transport-neutral streaming library for **gzip**, **Zstandard**, and
**Brotli**. It does not know about HTTP, HTTP/2, gRPC, TLS, files, or sockets.
Integration with the HTTP/2 response path is intentionally not done yet.

## Why streaming

All three formats are streaming codecs. A message may arrive in many chunks and
may be larger than any single buffer. The module keeps codec state per context
and lets the caller supply bounded input and output buffers:

```text
input chunks -> encoder context -> output chunks
output chunks <- decoder context <- input chunks
```

One context represents one direction of one stream. Contexts are not
thread-safe; use one per connection or stream and drive it from one thread.

## Public API

```c
typedef enum {
  COMPRESS_GZIP,
  COMPRESS_ZSTD,
  COMPRESS_BROTLI,
} compress_algorithm_t;

typedef enum {
  COMPRESS_FAST,
  COMPRESS_BALANCED,
  COMPRESS_BEST,
} compress_profile_t;

compress_ctx_t *compress_encoder_new(compress_algorithm_t algorithm,
                                     compress_profile_t profile);

compress_ctx_t *compress_decoder_new(compress_algorithm_t algorithm,
                                     size_t max_output);

compress_status_t compress_ctx_run(compress_ctx_t *ctx,
                                   const uint8_t *in, size_t in_len,
                                   size_t *in_used,
                                   uint8_t *out, size_t out_cap,
                                   size_t *out_used,
                                   int finish);
```

`COMPRESS_OK` means progress was made and the caller should continue.
`COMPRESS_DONE` means the stream ended. `COMPRESS_ERROR` means invalid input,
library failure, or bad arguments. `COMPRESS_LIMIT` means a decoder exceeded its
configured output cap.

### Encoder loop

```c
while (!done) {
  size_t in_used = 0, out_used = 0;

  status = compress_ctx_run(encoder, input + input_off, input_left,
                            &in_used, output, output_cap, &out_used,
                            finish);
  input_off += in_used;
  write(output, out_used);

  if (status == COMPRESS_DONE) {
    done = 1;
  } else if (status != COMPRESS_OK) {
    fail();
  }
}
```

Use `finish = 0` while more input remains. On the last input chunk, use
`finish = 1`. If the output buffer fills, call again with the remaining input
and `finish = 1`; after input is exhausted, call repeatedly with `in_len = 0`
until `COMPRESS_DONE`.

### Decoder loop

```c
while (!done) {
  size_t in_used = 0, out_used = 0;

  status = compress_ctx_run(decoder, input + input_off, input_left,
                            &in_used, output, output_cap, &out_used, 0);
  input_off += in_used;
  write(output, out_used);

  if (status == COMPRESS_DONE) {
    done = 1;
  } else if (status == COMPRESS_LIMIT || status == COMPRESS_ERROR) {
    fail();
  }
}
```

`finish` is ignored for decoders. `COMPRESS_DONE` means the compressed end marker
was reached. Any unconsumed input is left in the caller's buffer; the caller can
inspect it for trailing data.

## How gzip works

Gzip is a wrapper around DEFLATE:

1. The input is compressed with DEFLATE, which combines LZ77 back-references
   with Huffman coding.
2. The gzip header records metadata and compression method.
3. A CRC32 and uncompressed size are stored in the trailer.

This module uses zlib's streaming `deflate` and `inflate` APIs:

- Encoder: raw DEFLATE blocks inside a gzip wrapper (`windowBits = 15 + 16`).
- Decoder: gzip/zlib auto-detection (`windowBits = 15 + 32`).
- Profiles: fast = level 1, balanced = level 6, best = level 9.

Gzip is the compatibility choice. Almost every HTTP client and RPC stack
supports it. It is slower and usually produces larger output than zstd at a
similar CPU budget.

The gzip CRC detects accidental corruption, but it is not a security
authenticator. Use TLS or an application-level integrity mechanism for
authenticity.

## How Zstandard works

Zstandard (zstd) is a modern LZ-style compressor designed around speed and
tunable ratio:

- It uses match finding plus finite-state entropy coding.
- It supports fast and high-compression levels with a large tuning range.
- Frames can include checksums, though this module does not request one.
- It is excellent for dynamic RPC/JSON payloads where CPU latency matters.

This module uses zstd's streaming API:

- Encoder: `ZSTD_compressStream2` with `ZSTD_e_continue` or `ZSTD_e_end`.
- Decoder: `ZSTD_decompressStream`.
- Profiles: fast = level 1, balanced = level 3, best = level 19.

Level 19 is much slower and is intended for cached or bandwidth-expensive
content, not every request.

## How Brotli works

Brotli is a LZ-style compressor with a built-in static dictionary and context
modeling tuned for text:

- It often produces the smallest output for HTML, CSS, JavaScript, and JSON.
- The static dictionary helps with common web tokens.
- High quality levels are significantly slower than gzip or zstd.
- It is less universally available in older RPC clients.

This module uses Brotli's streaming encoder and decoder:

- Encoder: `BrotliEncoderCompressStream` with `PROCESS` or `FINISH`.
- Decoder: `BrotliDecoderDecompressStream`.
- Profiles: fast = quality 1, balanced = quality 5, best = quality 11.

Brotli is usually best for cacheable text/static assets. For hot, small RPC
responses, zstd or gzip is usually the better default.

## Profile mapping

| Profile | gzip | zstd | Brotli | Intended use |
|---|---:|---:|---:|---|
| `COMPRESS_FAST` | 1 | 1 | 1 | latency-sensitive dynamic data |
| `COMPRESS_BALANCED` | 6 | 3 | 5 | general default |
| `COMPRESS_BEST` | 9 | 19 | 11 | cacheable/static, bandwidth-bound |

Profiles are not a promise of identical CPU or ratio. They express intent; the
underlying libraries still have different cost curves.

## Decoder output limits

`compress_decoder_new()` requires a nonzero `max_output`. Every decoder tracks
total plaintext output and returns `COMPRESS_LIMIT` when the cap is reached.
This is a decompression-bomb defense and is not optional.

The caller should also bound compressed input size and processing time. A
compressed stream can be valid but still consume significant CPU before it hits
the output limit.

## Copies, memory, and CPU

Compression is a transform. For encoders, plaintext is read and compressed
output is written to the caller's output buffer. For decoders, compressed input
is read and plaintext is written to the caller's output buffer. The module does
not allocate a large per-message output buffer; the caller controls output
memory.

Expect:

- gzip: low CPU at fast level; moderate ratio; small codec state.
- zstd: very fast at low levels; good ratio; moderate codec state.
- Brotli: slower at high quality; best text ratio; moderate-to-high codec state.

The codec libraries may allocate internally. Compression and decompression each
add CPU work and data movement on top of the transport path.

## Security and HTTP integration notes

The module itself does not implement HTTP negotiation. When it is integrated:

- Parse `Accept-Encoding` and choose only supported algorithms.
- Set `Content-Encoding` and `Vary: Accept-Encoding`.
- Do not compress already-compressed content.
- Use a byte-size threshold, not an array-length threshold.
- Keep compression state across a response stream; do not compress each message
  independently unless the protocol explicitly requires per-message compression.
- Do not compress secrets reflected with attacker-controlled input.
- Enforce the decoder output cap and a compressed-input limit.

For gRPC, compression is per-message and negotiated separately from HTTP
content encoding. That integration will need to preserve message boundaries and
the compressed/uncompressed flag per message.

## Current status

Compression is integrated as HTTP middleware, not inside the HTTP/2 core:

- `http_mw_compression` implements request decompression and response compression.
- `http_exchange_t.compression` selects off/gzip/zstd/brotli.
- `http_exchange_t.compression_profile` selects fast/balanced/best.
- `http_exchange_t.max_decompressed` bounds decoder output.
- The middleware transforms DATA spans, so body framing and handlers see decoded
  request bytes and the writer sees encoded response bytes.
- No `Content-Encoding` or `Accept-Encoding` headers are used; both peers share
  the route contract.

The demo routes currently leave compression off. Enabling it requires setting
the exchange compression policy from the router/application layer. There is no
runtime negotiation, so mismatched deployments will fail to decode bodies.
