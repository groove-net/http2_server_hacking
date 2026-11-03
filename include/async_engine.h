#ifndef ASYNC_ENGINE_H
#define ASYNC_ENGINE_H

#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>

#define MAX_EVENTS 1024

typedef enum {
  TASK_ACTIVE,     // engine-managed reads and/or writes may make progress
  TASK_READY,      // explicitly scheduled: run process_handler next
  TASK_PROCESSING, // an I/O event is ready for process_handler
  TASK_WAIT_WRITE, // handler-owned output is waiting for writability
  TASK_COMPLETED   // finish and release the task
} task_state_t;

struct io_task; // Forward declaration

typedef struct io_task {
  int fd; // must be non-blocking before attach
  task_state_t state;

  // Caller-owned read ring. The engine appends at the wrapped tail; the
  // handler consumes from read_head.
  uint8_t *read_buffer;
  size_t read_size;      // capacity of read_buffer
  size_t read_head;      // first unconsumed byte
  size_t bytes_buffered; // bytes currently stored in the ring

  // Caller-owned write storage. The engine only reads from it.
  uint8_t *write_buffer; // borrowed active write; NULL if no write is active
  size_t write_size;     // capacity of write_buffer
  size_t write_len;      // total bytes to send in this write request
  size_t write_off;      // bytes of this write request already sent
  int write_active;      // engine owns this write request until write_done
  int write_done; // active write finished; handler may start the next request

  // Handler-visible lifecycle events.
  int read_closed; // peer EOF observed; read interest may be paused/rearmed

  void *context; // Caller context passed to on_complete/on_cancel

  void (*process_handler)(struct io_task *task);
  void (*on_complete)(void *context, int status, uint8_t *read_buffer);
  void (*on_cancel)(void *context); // optional cleanup for task_cancel()
} io_task_t;

// Async engine handle: one per worker thread. Backend-neutral. The concrete
// I/O mechanism (epoll, io_uring, ...) is chosen at creation and never leaks
// into this interface.
typedef struct io_loop io_loop_t;
typedef struct io_timer io_timer_t;

typedef enum {
  IO_BACKEND_AUTO = 0, // pick the best available for this host
  IO_BACKEND_EPOLL,
  IO_BACKEND_URING,
} io_backend_kind_t;

// Allocate a zeroed task and its internal engine entry. Set fd, context,
// handlers, and callbacks after creation. Read storage is installed later with
// task_setup_read(). If io_loop_add_task() fails, release it with task_free().
// After a successful add, the engine owns the task.
io_task_t *task_create(void);

// Release a task that has not been successfully added to a loop. Never call
// this after add: completion/cancellation releases the task automatically.
// Closes task->fd when it is non-negative. Buffers remain caller-owned.
void task_free(io_task_t *task);

// Schedule a task's process_handler to run at the next opportunity, even with
// no fd event. Must be called from the worker thread that owns the task; safe
// to call from within any handler (including the task's own, to self-resume).
void task_ready(io_task_t *task);

// Install caller-owned ring storage for the read side. Must be called before
// task_start_read() for data tasks. It may replace the buffer only when reads
// are stopped and the ring is empty. Returns 0, -EBUSY, or -EINVAL.
int task_setup_read(io_task_t *task, uint8_t *read_buffer, size_t read_size);

// Start or restart the read side: clear a prior EOF/stop state, arm read
// interest, and schedule the task. The engine then reads until EAGAIN, the ring
// fills, or EOF. Idempotent.
// Returns 0 on success, -EPIPE if the task is completing, and -EINVAL for bad
// arguments or a missing worker.
int task_start_read(io_task_t *task);

// Arm write readiness without an active write. Used for nonblocking connect
// and protocols that want to wait for writability before producing output.
void task_wait_write(io_task_t *task);

// Stop read interest without completing the task. Buffered bytes remain
// available to the handler, and the write side continues. Idempotent.
void task_stop_read(io_task_t *task);

// Available read bytes before the ring wraps. The handler reads from
// read_buffer + read_head; if this span is smaller than bytes_buffered, the
// rest is at read_buffer[0..bytes_buffered - span).
static inline size_t task_read_span(const io_task_t *task) {
  if (task == NULL || task->read_size == 0) {
    return 0;
  }
  size_t span = task->read_size - task->read_head;
  return span < task->bytes_buffered ? span : task->bytes_buffered;
}

// Consume n bytes from the front of the read ring. Returns 0 on success and
// -EINVAL if n exceeds bytes_buffered.
int task_read_consume(io_task_t *task, size_t n);

// Start one caller-owned write request. Does not copy buf; buf must remain
// valid and unchanged until write_done is set (or the task completes).
// Returns 0 on success, -EALREADY if a write is already active, -EPIPE if the
// write side is closed, and -EINVAL for bad arguments or a missing worker.
// Must be called from the worker thread that owns the task.
int task_write(io_task_t *task, const uint8_t *buf, size_t len);

// Cancel a task: run on_cancel() (or on_complete() if unset), then release the
// fd and task. Buffers belong to the caller and are not freed by the engine.
// Safe to call from inside any handler on the same worker, including the
// task's own handler (the release is then deferred until it returns).
void task_cancel(io_task_t *task);

// Whether cancellation has been requested for this task. Useful to shutdown
// code that must skip a task whose completion is already pending.
int is_task_cancel_requested(const io_task_t *task);

// Create/destroy a per-thread engine. io_loop_new() also installs the loop as
// the calling thread's current loop (used by task_ready/task_cancel).
io_loop_t *io_loop_new(io_backend_kind_t kind);
io_loop_t *io_loop_current(void);
const char *io_loop_backend_name(const io_loop_t *loop);

// Bind a task's fd to the loop's backend. Attach registers no readiness
// interest; call task_start_read() and task_write() for that. A successful
// attach schedules the task's first drive, so no manual kickoff is needed. The
// fd must already be non-blocking: the engine's read/write steps rely on EAGAIN
// instead of blocking the worker. The engine detaches the fd internally before
// closing it.
int io_loop_add_task(io_loop_t *loop, io_task_t *task);

// Run the loop until io_loop_exit() is called. If no stop handler is installed,
// io_loop_request_stop() also exits immediately.
void io_loop_run(io_loop_t *loop);

// Forced shutdown: cancel every task, run callbacks, drain in-flight backend
// operations, reap all task entries, and release the loop. Must be called on
// the loop's worker thread, outside io_loop_run() and outside a handler.
void io_loop_shutdown(io_loop_t *loop);

// One-shot timer. delay_ms is relative to now. The callback runs on the loop
// thread. Returns NULL on allocation failure or bad arguments. Cancel with
// io_timer_cancel(); a fired or cancelled timer handle must not be reused.
io_timer_t *io_timer_add(io_loop_t *loop, uint64_t delay_ms, void (*cb)(void *),
                         void *ctx);
void io_timer_cancel(io_timer_t *timer);

// Register the loop-thread callback invoked once when request_stop() is
// observed. Without a stop handler, request_stop() exits immediately.
void io_loop_set_stop_handler(io_loop_t *loop, void (*cb)(io_loop_t *));

// Ask the loop to begin shutdown. Safe from any thread: it sets the stop flag
// and wakes the backend. With a stop handler, the handler owns the drain and
// decides when to call io_loop_exit(); without one the loop exits.
void io_loop_request_stop(io_loop_t *loop);

// Whether io_loop_request_stop() has been called. A stop handler can use this
// while draining.
int io_loop_stop_requested(const io_loop_t *loop);

// Make io_loop_run() return after the current iteration. Safe from any thread.
void io_loop_exit(io_loop_t *loop);

#endif
