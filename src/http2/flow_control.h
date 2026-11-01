#ifndef FLOW_CONTROL_H
#define FLOW_CONTROL_H

/* Flow-control policy engine.
 *
 * This module decides when a receive window should be credited and by how
 * much. It does not know about sockets, nghttp2, HTTP/2 streams, or WINDOW_UPDATE
 * frames. A caller owns one flow_window_t per HTTP/2 connection and per stream,
 * feeds it events, and applies the returned action with nghttp2:
 *
 *   exact action  -> nghttp2_session_consume()
 *   top-up action -> nghttp2_session_set_local_window_size()
 *
 * The four policy choices are independent configuration axes but not every
 * combination is safe. flow_policy_validate() rejects invalid combinations and
 * reports warnings for risky but valid ones.
 *
 * A window tracks the receive capacity advertised to the peer. "current" is
 * the amount the peer may still send, "target" is the desired capacity,
 * "received" counts inbound DATA, "consumed" counts application progress, and
 * "eligible" counts bytes that may cause a credit according to the policy.
 */

#include <stddef.h>
#include <stdint.h>

#define FLOW_MAX_WINDOW 0x7fffffffu

typedef enum {
  FLOW_PROFILE_DEFAULT = 0,
  FLOW_PROFILE_LOW_LATENCY,
  FLOW_PROFILE_BULK,
  FLOW_PROFILE_ADAPTIVE,
  FLOW_PROFILE_OFF,
} flow_profile_t;

typedef enum {
  FLOW_TARGET_FIXED = 0,
  FLOW_TARGET_BDP,
} flow_target_t;

typedef enum {
  FLOW_SOURCE_RECEIPT = 0,   /* credit as soon as DATA arrives */
  FLOW_SOURCE_CONSUMPTION,   /* credit only after the app consumes bytes */
} flow_source_t;

typedef enum {
  FLOW_TRIGGER_IMMEDIATE = 0,
  FLOW_TRIGGER_THRESHOLD,
  FLOW_TRIGGER_LOW_WATER,
  FLOW_TRIGGER_TIMER,
} flow_trigger_t;

typedef enum {
  FLOW_AMOUNT_EXACT = 0, /* credit the exact eligible bytes */
  FLOW_AMOUNT_TOP_UP,    /* restore the window to target */
} flow_amount_t;

typedef struct flow_policy {
  int enabled;
  flow_target_t target;
  flow_source_t source;
  flow_trigger_t trigger;
  flow_amount_t amount;

  uint32_t stream_window;      /* initial/minimum per-stream target */
  uint32_t max_stream_window;  /* cap for BDP and stream target updates */
  uint32_t connection_window;  /* hard connection target cap */
  uint32_t min_update;         /* do not emit credits smaller than this */
  uint32_t threshold;          /* THRESHOLD trigger bytes */
  uint32_t low_water;          /* LOW_WATER trigger: current <= this */
  uint32_t timer_ms;           /* TIMER trigger period */
  size_t max_buffered_bytes;   /* 0 = caller has no explicit buffer budget */
} flow_policy_t;

typedef enum {
  FLOW_OK = 0,
  FLOW_ERR_WINDOW_ZERO,
  FLOW_ERR_WINDOW_ORDER,
  FLOW_ERR_WINDOW_MAX,
  FLOW_ERR_MIN_UPDATE,
  FLOW_ERR_THRESHOLD,
  FLOW_ERR_LOW_WATER,
  FLOW_ERR_TIMER,
  FLOW_ERR_BDP_AMOUNT,
  FLOW_ERR_ENUM,
} flow_error_t;

enum {
  FLOW_WARN_RECEIPT_NO_BUDGET = 1u << 0,
  FLOW_WARN_BDP_RECEIPT = 1u << 1,
  FLOW_WARN_TIMER_RECEIPT = 1u << 2,
  FLOW_WARN_LOW_WATER_EXACT = 1u << 3,
};

typedef enum {
  FLOW_CREDIT_NONE = 0,
  FLOW_CREDIT_EXACT,
  FLOW_CREDIT_TOP_UP,
} flow_credit_mode_t;

typedef struct flow_credit {
  flow_credit_mode_t mode;
  uint32_t bytes;  /* exact bytes, or the credit delta for top-up */
  uint32_t window; /* resulting absolute window for top-up */
} flow_credit_t;

typedef struct flow_window {
  const flow_policy_t *policy;
  uint32_t target;
  uint32_t current;
  uint64_t received;
  uint64_t consumed;
  uint64_t eligible;
  uint64_t target_raise;
  uint64_t next_timer_ms;
  uint64_t last_update_ms;
  int target_dirty;
} flow_window_t;

/* Fill a profile. stream_window and connection_window must be nonzero. */
void flow_policy_profile(flow_profile_t profile, uint32_t stream_window,
                         uint32_t connection_window, flow_policy_t *out);

/* Return FLOW_OK or the first hard error. Warnings are ORed into *warnings when
 * non-NULL. A disabled policy returns FLOW_OK with no warnings. */
flow_error_t flow_policy_validate(const flow_policy_t *policy,
                                  uint32_t *warnings);

/* Human-readable error string. */
const char *flow_error_string(flow_error_t error);

/* Connection target derived from active stream count and stream target.
 * Returns 0 when active_streams is 0. */
uint32_t flow_connection_target(const flow_policy_t *policy,
                                uint32_t active_streams, uint32_t stream_target);

/* Initialize a window at target. Returns 0, or -1 for bad arguments. */
int flow_window_init(flow_window_t *window, const flow_policy_t *policy,
                     uint32_t target);

/* Update the target. Increases may be applied by top-up; decreases take effect
 * as the window drains. */
void flow_window_set_target(flow_window_t *window, uint32_t target);

/* Events. Call after the corresponding bytes have been accepted/consumed. */
void flow_window_on_received(flow_window_t *window, uint32_t bytes);
void flow_window_on_consumed(flow_window_t *window, uint32_t bytes);

/* Evaluate the trigger and, if it fires, update the window and return the
 * credit the caller must apply. Call after events and periodically for
 * FLOW_TRIGGER_TIMER. A returned credit is already accounted in the window, so
 * the adapter must apply it or tear the connection down. */
flow_credit_t flow_window_update(flow_window_t *window, uint64_t now_ms);

#endif
