#include "flow_control.h"
#include <string.h>

static int flow_target_valid(flow_target_t target) {
  return target == FLOW_TARGET_FIXED || target == FLOW_TARGET_BDP;
}

static int flow_source_valid(flow_source_t source) {
  return source == FLOW_SOURCE_RECEIPT || source == FLOW_SOURCE_CONSUMPTION;
}

static int flow_trigger_valid(flow_trigger_t trigger) {
  return trigger == FLOW_TRIGGER_IMMEDIATE ||
         trigger == FLOW_TRIGGER_THRESHOLD ||
         trigger == FLOW_TRIGGER_LOW_WATER || trigger == FLOW_TRIGGER_TIMER;
}

static int flow_amount_valid(flow_amount_t amount) {
  return amount == FLOW_AMOUNT_EXACT || amount == FLOW_AMOUNT_TOP_UP;
}

void flow_policy_profile(flow_profile_t profile, uint32_t stream_window,
                         uint32_t connection_window, flow_policy_t *out) {
  if (out == NULL) {
    return;
  }
  memset(out, 0, sizeof *out);
  out->enabled = 1;
  out->target = FLOW_TARGET_FIXED;
  out->source = FLOW_SOURCE_CONSUMPTION;
  out->trigger = FLOW_TRIGGER_IMMEDIATE;
  out->amount = FLOW_AMOUNT_EXACT;
  out->stream_window = stream_window;
  out->max_stream_window = stream_window;
  out->connection_window = connection_window;
  out->min_update = 1;
  out->low_water = 0;
  out->max_buffered_bytes = 0;

  switch (profile) {
  case FLOW_PROFILE_LOW_LATENCY:
    out->source = FLOW_SOURCE_CONSUMPTION;
    out->trigger = FLOW_TRIGGER_IMMEDIATE;
    out->amount = FLOW_AMOUNT_EXACT;
    out->min_update = 1;
    out->low_water = 0;
    break;
  case FLOW_PROFILE_BULK:
    out->source = FLOW_SOURCE_CONSUMPTION;
    out->trigger = FLOW_TRIGGER_LOW_WATER;
    out->amount = FLOW_AMOUNT_TOP_UP;
    break;
  case FLOW_PROFILE_ADAPTIVE:
    out->target = FLOW_TARGET_BDP;
    out->source = FLOW_SOURCE_CONSUMPTION;
    out->trigger = FLOW_TRIGGER_LOW_WATER;
    out->amount = FLOW_AMOUNT_TOP_UP;
    out->max_stream_window = connection_window;
    break;
  case FLOW_PROFILE_OFF:
    out->enabled = 0;
    break;
  case FLOW_PROFILE_DEFAULT:
  default:
    break;
  }
}

flow_error_t flow_policy_validate(const flow_policy_t *p, uint32_t *warnings) {
  if (warnings != NULL) {
    *warnings = 0;
  }
  if (p == NULL || !p->enabled) {
    return FLOW_OK;
  }
  if (!flow_target_valid(p->target) || !flow_source_valid(p->source) ||
      !flow_trigger_valid(p->trigger) || !flow_amount_valid(p->amount)) {
    return FLOW_ERR_ENUM;
  }
  if (p->stream_window == 0 || p->connection_window == 0 ||
      p->max_stream_window == 0) {
    return FLOW_ERR_WINDOW_ZERO;
  }
  if (p->stream_window > p->connection_window ||
      p->max_stream_window < p->stream_window ||
      p->max_stream_window > p->connection_window) {
    return FLOW_ERR_WINDOW_ORDER;
  }
  if (p->connection_window > FLOW_MAX_WINDOW ||
      p->max_stream_window > FLOW_MAX_WINDOW) {
    return FLOW_ERR_WINDOW_MAX;
  }
  if (p->min_update > p->stream_window) {
    return FLOW_ERR_MIN_UPDATE;
  }
  if (p->trigger == FLOW_TRIGGER_THRESHOLD) {
    if (p->threshold == 0 || p->threshold > p->stream_window) {
      return FLOW_ERR_THRESHOLD;
    }
  }
  if (p->trigger == FLOW_TRIGGER_LOW_WATER &&
      p->low_water >= p->stream_window) {
    return FLOW_ERR_LOW_WATER;
  }
  if (p->trigger == FLOW_TRIGGER_TIMER && p->timer_ms == 0) {
    return FLOW_ERR_TIMER;
  }
  if (p->target == FLOW_TARGET_BDP && p->amount != FLOW_AMOUNT_TOP_UP) {
    return FLOW_ERR_BDP_AMOUNT;
  }
  uint32_t w = 0;
  if (p->source == FLOW_SOURCE_RECEIPT && p->max_buffered_bytes == 0) {
    w |= FLOW_WARN_RECEIPT_NO_BUDGET;
  }
  if (p->target == FLOW_TARGET_BDP && p->source == FLOW_SOURCE_RECEIPT) {
    w |= FLOW_WARN_BDP_RECEIPT;
  }
  if (p->trigger == FLOW_TRIGGER_TIMER &&
      p->source == FLOW_SOURCE_RECEIPT) {
    w |= FLOW_WARN_TIMER_RECEIPT;
  }
  if (p->trigger == FLOW_TRIGGER_LOW_WATER &&
      p->amount == FLOW_AMOUNT_EXACT) {
    w |= FLOW_WARN_LOW_WATER_EXACT;
  }
  if (warnings != NULL) {
    *warnings = w;
  }
  return FLOW_OK;
}

const char *flow_error_string(flow_error_t error) {
  switch (error) {
  case FLOW_OK:
    return "ok";
  case FLOW_ERR_WINDOW_ZERO:
    return "window sizes must be nonzero";
  case FLOW_ERR_WINDOW_ORDER:
    return "stream window must be <= max stream window <= connection window";
  case FLOW_ERR_WINDOW_MAX:
    return "window exceeds the HTTP/2 maximum";
  case FLOW_ERR_MIN_UPDATE:
    return "min_update exceeds the stream window";
  case FLOW_ERR_THRESHOLD:
    return "threshold must be in (0, stream_window]";
  case FLOW_ERR_LOW_WATER:
    return "low_water must be in (0, stream_window)";
  case FLOW_ERR_TIMER:
    return "timer trigger requires timer_ms > 0";
  case FLOW_ERR_BDP_AMOUNT:
    return "BDP target requires top-up credit";
  case FLOW_ERR_ENUM:
  default:
    return "invalid flow-control enum";
  }
}

uint32_t flow_connection_target(const flow_policy_t *p,
                                uint32_t active_streams,
                                uint32_t stream_target) {
  if (p == NULL || !p->enabled || active_streams == 0 || stream_target == 0) {
    return 0;
  }
  uint64_t sum = (uint64_t)active_streams * stream_target;
  uint64_t cap = p->connection_window;
  if (sum > cap) {
    sum = cap;
  }
  if (sum < p->stream_window) {
    sum = p->stream_window;
  }
  return (uint32_t)sum;
}

int flow_window_init(flow_window_t *w, const flow_policy_t *p, uint32_t target) {
  if (w == NULL || p == NULL || target == 0) {
    return -1;
  }
  memset(w, 0, sizeof *w);
  w->policy = p;
  w->target = target;
  w->current = target;
  w->next_timer_ms = 0;
  w->last_update_ms = 0;
  w->target_dirty = 0;
  w->target_raise = 0;
  return 0;
}

void flow_window_set_target(flow_window_t *w, uint32_t target) {
  if (w == NULL || target == 0) {
    return;
  }
  if (target > w->target) {
    w->target_raise += (uint64_t)target - w->target;
    w->target_dirty = 1;
  } else if (target < w->target) {
    w->target_dirty = 0;
  }
  w->target = target;
}

void flow_window_on_received(flow_window_t *w, uint32_t bytes) {
  if (w == NULL || bytes == 0) {
    return;
  }
  w->received += bytes;
  w->current = bytes < w->current ? w->current - bytes : 0;
  if (w->policy != NULL && w->policy->source == FLOW_SOURCE_RECEIPT) {
    w->eligible += bytes;
  }
}

void flow_window_on_consumed(flow_window_t *w, uint32_t bytes) {
  if (w == NULL || bytes == 0) {
    return;
  }
  w->consumed += bytes;
  if (w->policy != NULL && w->policy->source == FLOW_SOURCE_CONSUMPTION) {
    w->eligible += bytes;
  }
}

static int flow_trigger_fires(flow_window_t *w, uint64_t now_ms) {
  const flow_policy_t *p = w->policy;
  switch (p->trigger) {
  case FLOW_TRIGGER_IMMEDIATE:
    return w->eligible != 0 || w->target_dirty;
  case FLOW_TRIGGER_THRESHOLD:
    return w->eligible >= p->threshold;
  case FLOW_TRIGGER_LOW_WATER:
    return w->current <= p->low_water && (w->eligible != 0 || w->target_dirty);
  case FLOW_TRIGGER_TIMER:
    if (now_ms < w->next_timer_ms) {
      return 0;
    }
    w->next_timer_ms = now_ms + p->timer_ms;
    return w->eligible != 0 || w->target_dirty;
  default:
    return 0;
  }
}

flow_credit_t flow_window_update(flow_window_t *w, uint64_t now_ms) {
  flow_credit_t none = {FLOW_CREDIT_NONE, 0, 0};
  if (w == NULL || w->policy == NULL || !w->policy->enabled) {
    return none;
  }
  const flow_policy_t *p = w->policy;
  if (!flow_trigger_fires(w, now_ms)) {
    return none;
  }

  uint32_t deficit = w->target > w->current ? w->target - w->current : 0;
  uint64_t credit = 0;
  flow_credit_mode_t mode = FLOW_CREDIT_NONE;

  if (p->amount == FLOW_AMOUNT_EXACT) {
    credit = w->eligible;
    if (credit > deficit) {
      credit = deficit;
    }
    if (credit == 0) {
      return none;
    }
    if (credit < p->min_update && w->current != 0) {
      return none;
    }
    mode = FLOW_CREDIT_EXACT;
    w->eligible -= credit;
    if (w->current == w->target) {
      w->target_dirty = 0;
    }
  } else {
    uint64_t allowed = w->eligible + w->target_raise;
    credit = deficit;
    if (credit > allowed) {
      credit = allowed;
    }
    if (credit == 0) {
      return none;
    }
    if (credit < p->min_update && w->current != 0) {
      return none;
    }
    mode = FLOW_CREDIT_TOP_UP;
    uint64_t from_raise = w->target_raise < credit ? w->target_raise : credit;
    w->target_raise -= from_raise;
    uint64_t from_eligible = credit - from_raise;
    w->eligible = w->eligible > from_eligible ? w->eligible - from_eligible : 0;
    w->target_dirty = w->target_raise != 0;
  }

  w->current += (uint32_t)credit;
  w->last_update_ms = now_ms;
  flow_credit_t result = {mode, (uint32_t)credit, w->current};
  return result;
}
