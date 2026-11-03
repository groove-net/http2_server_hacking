#ifndef ENGINE_INTERNAL_H
#define ENGINE_INTERNAL_H

#include "async_engine.h"
#include <stdatomic.h>
#include <stddef.h>

// Private per-task state. The public io_task_t is the first member, so a task
// pointer can be recovered from its entry with a plain cast.
typedef struct io_task_entry {
  io_task_t task;

  // Scheduler.
  io_task_t *ready_prev;
  io_task_t *ready_next;
  int ready_queued;
  int cancel_requested;
  int cancel_pending;
  struct io_task_entry *cancel_prev;
  struct io_task_entry *cancel_next;
  int io_error;

  // Registry and deferred release. task_finish() unlinks immediately and
  // queues the entry on removed_head; loop_reap() frees that list after the
  // current event batch.
  int removed;
  int attached;
  struct io_loop *owner;
  struct io_task_entry *tasks_prev;
  struct io_task_entry *next;
  struct io_task_entry *removed_next;

  // Backend operations and watches.
  int read_op_ready;
  int read_op_result;
  int read_inflight;
  int write_op_ready;
  int write_op_result;
  int write_inflight;
  unsigned ready_events;
  int read_stopped;
  int watch_armed;
  int read_watch_armed;
  int write_watch_armed;
  int inflight_ops;
  int batch_queued;

  // Duplex state-machine guards.
  int read_eof_pending;
  int read_paused;
  int write_blocked;
  int io_abort;
} io_task_entry_t;

_Static_assert(offsetof(io_task_entry_t, task) == 0,
               "io_task_t must be first in io_task_entry_t");

static inline io_task_entry_t *task_entry(io_task_t *task) {
  return (io_task_entry_t *)(void *)task;
}

static inline const io_task_entry_t *task_entry_const(const io_task_t *task) {
  return (const io_task_entry_t *)(const void *)task;
}

static inline size_t task_read_tail(const io_task_t *task) {
  if (task->read_size == 0) {
    return 0;
  }
  return (task->read_head + task->bytes_buffered) % task->read_size;
}

// Free contiguous bytes starting at the wrapped tail.
static inline size_t task_read_free_span(const io_task_t *task) {
  if (task->read_size == 0 || task->bytes_buffered >= task->read_size) {
    return 0;
  }
  size_t tail = task_read_tail(task);
  size_t free_total = task->read_size - task->bytes_buffered;
  size_t contiguous = task->read_size - tail;
  return free_total < contiguous ? free_total : contiguous;
}

// Internal readiness bits. Callers start operations with task_start_read() and
// task_write() instead of passing an attach mask.
enum {
  READY_READ  = 1u << 0,
  READY_WRITE = 1u << 1,
};

// Backend step results returned by read_step()/write_step().
enum {
  IO_STEP_DONE = 0, // transferred bytes (bytes_buffered/write_off advanced)
  IO_STEP_AGAIN,    // would block
  IO_STEP_EOF,      // peer closed / zero bytes
  IO_STEP_ERROR,    // hard error
  IO_STEP_PENDING,  // operation submitted; completion will re-drive the task
};

struct io_loop;

typedef struct io_timer {
  struct io_loop *loop;
  uint64_t deadline_ms;
  void (*cb)(void *ctx);
  void *ctx;
  size_t index;
} io_timer_t;

// A pluggable I/O mechanism. The engine never calls epoll/io_uring directly;
// it only drives this vtable, so the engine stays general and backend-neutral.
typedef struct io_backend {
  const char *name;
  int (*init)(struct io_loop *loop);
  void (*fini)(struct io_loop *loop);
  int (*attach)(struct io_loop *loop, io_task_t *task);
  int (*detach)(struct io_loop *loop, io_task_t *task);
  // Block until events/operations are ready (or timeout_ms elapses; < 0 means
  // wait forever); fill out[] with runnable tasks.
  int (*wait)(struct io_loop *loop, io_task_t **out, int max, int timeout_ms);
  // Submit any queued operations (io_uring); no-op for readiness backends.
  void (*flush)(struct io_loop *loop);
  int (*read_step)(struct io_loop *loop, io_task_t *task);
  int (*write_step)(struct io_loop *loop, io_task_t *task);
  // Enable/disable readable/writable interest on demand.
  int (*read_watch)(struct io_loop *loop, io_task_t *task, int enable);
  int (*write_watch)(struct io_loop *loop, io_task_t *task, int enable);
  // Called when a task parks (yields without completing). Proactor backends
  // use it to arm follow-up readiness (e.g. POLLOUT after a blocked flush).
  // May be NULL.
  int (*park)(struct io_loop *loop, io_task_t *task);
  // Called by task_cancel() when inflight_ops is nonzero. Must arrange for the
  // outstanding operations to complete (shutdown() the socket, close a polled
  // fd, ...) while leaving the task alive; drive_task() frees it when the
  // final completion is reaped. May be NULL for synchronous backends.
  void (*cancel)(struct io_loop *loop, io_task_t *task);
  // Interrupt a blocked wait() from another/explicit scheduling.
  void (*wake)(struct io_loop *loop);
} io_backend_t;

// Per-thread engine. Defined here so backends can reach `impl` and the ready
// queue; consumers only ever see the opaque io_loop_t.
struct io_loop {
  const io_backend_t *backend;
  void *impl; // backend-private state
  io_task_t *ready_head;
  io_task_t *ready_tail;
  io_task_entry_t *tasks;
  io_task_entry_t *removed_head;

  // Cross-thread lifecycle signalling. request_stop() asks the owner to begin
  // shutdown; exit() makes io_loop_run() return.
  void (*stop_cb)(struct io_loop *loop);
  int stop_notified;
  _Atomic int stop_requested;
  _Atomic int exit_requested;

  // Min-heap of one-shot deadlines.
  io_timer_t **timers;
  size_t timer_count;
  size_t timer_cap;
};

#endif
