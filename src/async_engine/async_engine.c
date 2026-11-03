#include "engine_internal.h"
#include <time.h>

static uint64_t engine_now_ms(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000u + (uint64_t)(ts.tv_nsec / 1000000L);
}

static void timer_swap(io_loop_t *loop, size_t a, size_t b) {
  io_timer_t *ta = loop->timers[a];
  io_timer_t *tb = loop->timers[b];
  loop->timers[a] = tb;
  loop->timers[b] = ta;
  tb->index = a;
  ta->index = b;
}

static void timer_sift_up(io_loop_t *loop, size_t i) {
  while (i > 0) {
    size_t parent = (i - 1) / 2;
    if (loop->timers[parent]->deadline_ms <= loop->timers[i]->deadline_ms) {
      break;
    }
    timer_swap(loop, i, parent);
    i = parent;
  }
}

static void timer_sift_down(io_loop_t *loop, size_t i) {
  for (;;) {
    size_t left = i * 2 + 1;
    size_t right = left + 1;
    size_t best = i;
    if (left < loop->timer_count &&
        loop->timers[left]->deadline_ms < loop->timers[best]->deadline_ms) {
      best = left;
    }
    if (right < loop->timer_count &&
        loop->timers[right]->deadline_ms < loop->timers[best]->deadline_ms) {
      best = right;
    }
    if (best == i) {
      break;
    }
    timer_swap(loop, i, best);
    i = best;
  }
}

static int timer_insert(io_loop_t *loop, io_timer_t *timer) {
  if (loop->timer_count == loop->timer_cap) {
    size_t cap = loop->timer_cap == 0 ? 16 : loop->timer_cap * 2;
    io_timer_t **next = realloc(loop->timers, cap * sizeof *next);
    if (next == NULL) {
      return -1;
    }
    loop->timers = next;
    loop->timer_cap = cap;
  }
  size_t i = loop->timer_count++;
  timer->index = i;
  loop->timers[i] = timer;
  timer_sift_up(loop, i);
  return 0;
}

static void timer_remove(io_loop_t *loop, io_timer_t *timer) {
  size_t i = timer->index;
  if (i >= loop->timer_count || loop->timers[i] != timer) {
    return;
  }
  size_t last = --loop->timer_count;
  if (i != last) {
    loop->timers[i] = loop->timers[last];
    loop->timers[i]->index = i;
    if (i > 0 &&
        loop->timers[i]->deadline_ms < loop->timers[(i - 1) / 2]->deadline_ms) {
      timer_sift_up(loop, i);
    } else {
      timer_sift_down(loop, i);
    }
  }
  timer->index = SIZE_MAX;
}

static void loop_run_timers(io_loop_t *loop, uint64_t now) {
  while (loop->timer_count > 0 && loop->timers[0]->deadline_ms <= now) {
    io_timer_t *timer = loop->timers[0];
    timer_remove(loop, timer);
    void (*cb)(void *) = timer->cb;
    void *ctx = timer->ctx;
    free(timer);
    cb(ctx);
  }
}

static int loop_timeout_ms(io_loop_t *loop, uint64_t now) {
  if (loop->timer_count == 0) {
    return -1;
  }
  uint64_t left = loop->timers[0]->deadline_ms > now
                      ? loop->timers[0]->deadline_ms - now
                      : 0;
  return left > 60000u ? 60000 : (int)left;
}

static void loop_free_timers(io_loop_t *loop) {
  for (size_t i = 0; i < loop->timer_count; i++) {
    free(loop->timers[i]);
  }
  free(loop->timers);
  loop->timers = NULL;
  loop->timer_count = 0;
  loop->timer_cap = 0;
}

// Concrete backends. Unity-style inclusion mirrors the rest of this project:
// the Makefile compiles only the module entry file, so the backends pull in
// through this translation unit.
#include "epoll_backend.c"
#include "uring_backend.c"

// --- Worker-local scheduler -------------------------------------------------
// Each worker thread owns a loop with a ready queue. A task can be made
// runnable from an fd/op event (delivered by the backend) or explicitly via
// task_ready(). Worker-local by design: work is pinned to a thread, so no
// locking is needed.
static _Thread_local io_loop_t *tl_loop = NULL;
static _Thread_local io_task_t *tl_current = NULL; // task inside drive_task

static void ready_unlink(io_task_t *task) {
  io_loop_t *loop = tl_loop;
  io_task_entry_t *entry = task_entry(task);

  if (loop == NULL || !entry->ready_queued) {
    return;
  }
  if (entry->ready_prev != NULL) {
    task_entry(entry->ready_prev)->ready_next = entry->ready_next;
  } else {
    loop->ready_head = entry->ready_next;
  }
  if (entry->ready_next != NULL) {
    task_entry(entry->ready_next)->ready_prev = entry->ready_prev;
  } else {
    loop->ready_tail = entry->ready_prev;
  }
  entry->ready_prev = NULL;
  entry->ready_next = NULL;
  entry->ready_queued = 0;
}

static void ready_enqueue_loop(io_loop_t *loop, io_task_t *task) {
  if (task == NULL || loop == NULL) {
    return;
  }
  io_task_entry_t *entry = task_entry(task);
  if (entry->ready_queued || entry->cancel_requested ||
      task->state == TASK_COMPLETED) {
    return;
  }
  entry->ready_prev = loop->ready_tail;
  entry->ready_next = NULL;
  if (loop->ready_tail != NULL) {
    task_entry(loop->ready_tail)->ready_next = task;
  } else {
    loop->ready_head = task;
  }
  loop->ready_tail = task;
  entry->ready_queued = 1;
}

static void ready_enqueue(io_task_t *task) {
  ready_enqueue_loop(tl_loop, task);
}

// Wake the engine without changing what the task is currently doing. Used by
// task_write(), which must not turn an ACTIVE task into a handler call by
// accident.
static void task_kick(io_task_t *task) {
  if (task == NULL || tl_loop == NULL || task_entry(task)->cancel_requested ||
      task->state == TASK_COMPLETED) {
    return;
  }
  ready_enqueue(task);
}

io_task_t *task_create(void) {
  io_task_entry_t *entry = calloc(1, sizeof *entry);
  if (entry == NULL) {
    return NULL;
  }
  entry->task.fd = -1;
  return &entry->task;
}

void task_free(io_task_t *task) {
  if (task == NULL) {
    return;
  }
  if (task->fd >= 0) {
    close(task->fd);
    task->fd = -1;
  }
  free(task_entry(task));
}

int is_task_cancel_requested(const io_task_t *task) {
  return task != NULL && task_entry_const(task)->cancel_requested;
}

void task_ready(io_task_t *task) {
  if (task == NULL) {
    return;
  }
  task->state = TASK_READY;
  ready_enqueue(task);
}

static int task_update_read_watch(io_task_t *task, int enable);
static int task_update_write_watch(io_task_t *task, int enable);

int task_setup_read(io_task_t *task, uint8_t *read_buffer, size_t read_size) {
  if (task == NULL || read_buffer == NULL || read_size == 0) {
    return -EINVAL;
  }
  io_task_entry_t *entry = task_entry(task);
  if (task->bytes_buffered != 0 || entry->read_inflight) {
    return -EBUSY;
  }
  if (task->read_buffer != NULL && !entry->read_stopped) {
    return -EBUSY;
  }
  task->read_buffer = read_buffer;
  task->read_size = read_size;
  task->read_head = 0;
  task->bytes_buffered = 0;
  return 0;
}

int task_start_read(io_task_t *task) {
  if (task == NULL || tl_loop == NULL) {
    return -EINVAL;
  }
  io_task_entry_t *entry = task_entry(task);
  if (entry->cancel_requested || task->state == TASK_COMPLETED) {
    return -EPIPE;
  }
  entry->read_stopped = 0;
  task->read_closed = 0;
  entry->read_eof_pending = 0;
  if (task->read_buffer == NULL) {
    task->state = TASK_PROCESSING; // readiness-only task
  }
  int rc = task_update_read_watch(task, 1);
  if (rc != 0) {
    entry->read_stopped = 1;
    return rc;
  }
  if (task != tl_current) {
    task_kick(task);
  }
  return 0;
}

void task_wait_write(io_task_t *task) {
  if (task == NULL || task->state == TASK_COMPLETED) {
    return;
  }
  task->state = TASK_WAIT_WRITE;
  task_update_write_watch(task, 1);
}

void task_stop_read(io_task_t *task) {
  if (task == NULL || task->state == TASK_COMPLETED) {
    return;
  }
  task_entry(task)->read_stopped = 1;
  task_update_read_watch(task, 0);
}

int task_read_consume(io_task_t *task, size_t n) {
  if (task == NULL || n > task->bytes_buffered) {
    return -EINVAL;
  }
  if (n == 0) {
    return 0;
  }
  task->read_head = (task->read_head + n) % task->read_size;
  task->bytes_buffered -= n;
  if (task->bytes_buffered == 0) {
    task->read_head = 0;
  }
  return 0;
}

int task_write(io_task_t *task, const uint8_t *buf, size_t len) {
  if (task == NULL || buf == NULL || len == 0 || tl_loop == NULL) {
    return -EINVAL;
  }
  io_task_entry_t *entry = task_entry(task);
  if (entry->owner != tl_loop) {
    return -EINVAL;
  }
  if (entry->cancel_requested || task->state == TASK_COMPLETED) {
    return -EPIPE;
  }
  if (task->state == TASK_WAIT_WRITE) {
    return -EBUSY;
  }
  if (task->write_active) {
    return -EALREADY;
  }
  task->write_buffer = (uint8_t *)buf;
  task->write_size = len;
  task->write_len = len;
  task->write_off = 0;
  task->write_active = 1;
  task->write_done = 0;
  entry->write_blocked = 0;
  task->state = TASK_ACTIVE;
  if (task != tl_current) {
    task_kick(task);
  }
  return 0;
}

static void registry_unlink(io_loop_t *loop, io_task_entry_t *entry) {
  if (entry->tasks_prev != NULL) {
    entry->tasks_prev->next = entry->next;
  } else {
    loop->tasks = entry->next;
  }
  if (entry->next != NULL) {
    entry->next->tasks_prev = entry->tasks_prev;
  }
  entry->tasks_prev = NULL;
  entry->next = NULL;
}

// Retire a finished/cancelled task. Marks the entry removed, detaches it, and
// runs the callback; loop_reap() calls task_free(), which closes the fd and
// releases the entry after the current batch. Must not run while the task is
// executing (i.e. while tl_current == task); task_cancel() defers in that case.
// The read/write buffers are caller-owned and are left untouched for
// on_complete()/on_cancel() to free.
static void task_finish(io_task_t *task, int cancelled) {
  io_loop_t *loop = tl_loop;
  io_task_entry_t *entry = task_entry(task);
  if (entry->removed) {
    return;
  }
  int was_attached = entry->attached;
  entry->removed = 1;
  if (loop != NULL && was_attached) {
    entry->attached = 0;
    entry->owner = NULL;
    registry_unlink(loop, entry);
    entry->removed_next = loop->removed_head;
    loop->removed_head = entry;
  }
  if (entry->ready_queued) {
    ready_unlink(task);
    entry->ready_queued = 0;
  }
  if (loop != NULL && loop->backend != NULL) {
    loop->backend->detach(loop, task); // before the fd is closed
  }
  if (cancelled && task->on_cancel != NULL) {
    task->on_cancel(task->context);
  } else if (task->on_complete != NULL) {
    task->on_complete(task->context,
                      cancelled ? ECANCELED : entry->io_error,
                      task->read_buffer);
  }
  if (loop == NULL || !was_attached) {
    task_free(task);
  }
}

// Ask the backend to force any outstanding operation to complete. The task
// stays alive until inflight_ops reaches zero and the completion is reaped.
static void task_abort_io(io_task_t *task) {
  io_loop_t *loop = tl_loop;
  io_task_entry_t *entry = task_entry(task);
  if (entry->io_abort) {
    return;
  }
  entry->io_abort = 1;
  if (entry->inflight_ops > 0 && loop != NULL && loop->backend != NULL &&
      loop->backend->cancel != NULL) {
    loop->backend->cancel(loop, task);
  }
}

// Returns 1 if the task was retired, 0 if it is waiting for outstanding I/O.
static int task_maybe_finish(io_task_t *task, int cancelled) {
  if (task_entry(task)->inflight_ops > 0) {
    task_abort_io(task);
    return 0;
  }
  task_finish(task, cancelled);
  return 1;
}

void task_cancel(io_task_t *task) {
  if (task == NULL || task->state == TASK_COMPLETED) {
    return;
  }
  io_task_entry_t *entry = task_entry(task);
  if (entry->cancel_requested) {
    return;
  }
  entry->cancel_requested = 1;
  if (task == tl_current) {
    return; // executing: drive_task() observes the flag and finishes it
  }
  if (entry->inflight_ops > 0) {
    task_abort_io(task);
    return; // freed when the final completion is reaped
  }
  task_finish(task, 1);
}

static int task_update_read_watch(io_task_t *task, int enable) {
  io_loop_t *loop = tl_loop;
  if (loop != NULL && loop->backend != NULL &&
      loop->backend->read_watch != NULL) {
    return loop->backend->read_watch(loop, task, enable);
  }
  return 0;
}

static void task_mark_read_closed(io_task_t *task) {
  task->read_closed = 1;
  task_entry(task)->read_eof_pending = 1;
  task_update_read_watch(task, 0);
}

static int task_update_write_watch(io_task_t *task, int enable) {
  io_loop_t *loop = tl_loop;
  if (loop != NULL && loop->backend != NULL &&
      loop->backend->write_watch != NULL) {
    return loop->backend->write_watch(loop, task, enable);
  }
  return 0;
}

static void task_complete_write(io_task_t *task) {
  task->write_active = 0;
  task->write_done = 1;
  task_entry(task)->write_blocked = 0;
  task_update_write_watch(task, 0);
}

enum read_drive_action {
  READ_DRIVE_CONTINUE,
  READ_DRIVE_PROCESS,
  READ_DRIVE_COMPLETE,
  READ_DRIVE_YIELD,
};

static enum read_drive_action drive_read(io_task_t *task, io_loop_t *loop,
                                         int allow_step) {
  io_task_entry_t *entry = task_entry(task);

  if (task->bytes_buffered == 0) {
    entry->read_paused = 0;
  }

  if (entry->read_op_ready) {
    int st = entry->read_op_result;
    entry->read_op_ready = 0;
    if (st == IO_STEP_ERROR) {
      return READ_DRIVE_COMPLETE;
    }
    if (st == IO_STEP_EOF) {
      task_mark_read_closed(task);
    }
  }

  if (task->bytes_buffered > 0 && !entry->read_paused) {
    return READ_DRIVE_PROCESS;
  }
  if (entry->read_eof_pending) {
    return READ_DRIVE_PROCESS;
  }

  if (allow_step && !task->read_closed && !entry->read_stopped &&
      !entry->read_inflight && task->read_buffer != NULL &&
      task->read_size > 0 && task->bytes_buffered < task->read_size) {
    int st = loop->backend->read_step(loop, task);
    if (st == IO_STEP_DONE) {
      entry->read_paused = 0;
      return READ_DRIVE_CONTINUE;
    }
    if (st == IO_STEP_ERROR) {
      return READ_DRIVE_COMPLETE;
    }
    if (st == IO_STEP_EOF) {
      task_mark_read_closed(task);
      return READ_DRIVE_PROCESS;
    }
    if (st == IO_STEP_AGAIN && task->bytes_buffered > 0 &&
        !entry->read_paused) {
      return READ_DRIVE_PROCESS;
    }
  }
  return READ_DRIVE_YIELD;
}

static void drive_task(io_task_t *task) {
  io_loop_t *loop = tl_loop;
  io_task_t *prev_current = tl_current;
  task_entry(task)->batch_queued = 0;

  if (task_entry(task)->removed) {
    return;
  }

  tl_current = task;

  // A completion or cancellation that was deferred behind in-flight backend
  // work is finished before any more state-machine work.
  if (task_entry(task)->cancel_requested || task->state == TASK_COMPLETED) {
    int cancelled = task_entry(task)->cancel_requested;
    tl_current = prev_current;
    task_maybe_finish(task, cancelled);
    return;
  }

  while (1) {
    switch (task->state) {

    case TASK_ACTIVE: {
      // A handler that left unprocessed input behind must not be re-entered
      // until it schedules itself. Clear the guard once the input is consumed.
      if (task->bytes_buffered == 0) {
        task_entry(task)->read_paused = 0;
      }

      switch (drive_read(task, loop, task->write_done == 0)) {
      case READ_DRIVE_CONTINUE:
        continue;
      case READ_DRIVE_PROCESS:
        task->state = TASK_PROCESSING;
        continue;
      case READ_DRIVE_COMPLETE:
        task->state = TASK_COMPLETED;
        continue;
      case READ_DRIVE_YIELD:
        break;
      }

      if (task->write_done) {
        task->state = TASK_PROCESSING;
        continue;
      }

      // Consume a proactor write completion.
      if (task_entry(task)->write_op_ready) {
        int st = task_entry(task)->write_op_result;
        task_entry(task)->write_op_ready = 0;
        task_entry(task)->write_blocked = 0;
        if (st == IO_STEP_ERROR) {
          task->state = TASK_COMPLETED;
          continue;
        }
        if (st == IO_STEP_AGAIN) {
          task_entry(task)->write_blocked = 1;
          goto yield;
        }
        if (task->write_off >= task->write_len) {
          task_complete_write(task);
          task->state = TASK_PROCESSING;
          continue;
        }
      }

      // Drive the active write. An idle write side is normal: it never
      // completes the task by itself.
      if (task->write_active) {
        if (task->write_off >= task->write_len) {
          task_complete_write(task);
          task->state = TASK_PROCESSING;
          continue;
        }
        if (!task_entry(task)->write_inflight) {
          int st = loop->backend->write_step(loop, task);
          if (st == IO_STEP_DONE) {
            if (task->write_off >= task->write_len) {
              task_complete_write(task);
              task->state = TASK_PROCESSING;
            }
            continue;
          }
          if (st == IO_STEP_PENDING || st == IO_STEP_AGAIN) {
            if (st == IO_STEP_AGAIN) {
              task_entry(task)->write_blocked = 1;
            }
            goto yield;
          }
          if (st == IO_STEP_ERROR) {
            task->state = TASK_COMPLETED;
            continue;
          }
        } else {
          goto yield;
        }
      }

      goto yield; // nothing ready: park
    }

    case TASK_WAIT_WRITE: {
      switch (drive_read(task, loop,
                            (task_entry(task)->ready_events & READY_WRITE) == 0)) {
      case READ_DRIVE_CONTINUE:
        continue;
      case READ_DRIVE_PROCESS:
        task->state = TASK_PROCESSING;
        continue;
      case READ_DRIVE_COMPLETE:
        task->state = TASK_COMPLETED;
        continue;
      case READ_DRIVE_YIELD:
        break;
      }

      // Writable readiness here always belongs to handler-owned output (for
      // example nghttp2's send callbacks), so it must run the handler.
      if ((task_entry(task)->ready_events & READY_WRITE) != 0) {
        task_entry(task)->ready_events &= ~READY_WRITE;
        task->state = TASK_READY;
        continue;
      }

      goto yield; // wait for writable readiness
    }

    // READY is the explicit-schedule entry (task_ready); PROCESSING is the
    // post-read/post-write entry. Both simply run the task's handler.
    case TASK_READY:
    case TASK_PROCESSING: {
      task_entry(task)->read_eof_pending = 0;
      task_entry(task)->read_paused = 0;

      // Execute FD-specific logic (timers, IPC, parsing, producers)
      if (task->process_handler) {
        task->process_handler(task);
      } else {
        task->state = TASK_COMPLETED;
      }
      if (task_entry(task)->cancel_requested) {
        task->state = TASK_COMPLETED;
      }

      if (task->state == TASK_READY || task->state == TASK_PROCESSING) {
        goto yield;
      }
      if (task->state == TASK_WAIT_WRITE) {
        // Writable readiness may have been consumed before this handler ran.
        // If readable readiness is still pending, process it now instead of
        // parking and dropping the edge-triggered EPOLLIN bit.
        if (task_entry(task)->ready_events != 0) {
          continue;
        }
        goto yield;
      }
      if (task->state == TASK_COMPLETED) {
        continue;
      }
      if (task->state == TASK_ACTIVE) {
        if (task->bytes_buffered > 0) {
          task_entry(task)->read_paused = 1;
        }
        continue;
      }
      // Unknown handler state: fail closed.
      task->state = TASK_COMPLETED;
      continue;
    }

    case TASK_COMPLETED: {
      int cancelled = task_entry(task)->cancel_requested;
      tl_current = prev_current;
      task_maybe_finish(task, cancelled);
      return; // destroyed, or waiting for outstanding I/O to be reaped
    }
    }
  }

yield:
  task_entry(task)->ready_events = 0;
  int park_error = 0;
  if (loop != NULL && loop->backend != NULL && loop->backend->park != NULL) {
    park_error = loop->backend->park(loop, task);
  }
  tl_current = prev_current;
  if (park_error != 0) {
    task_entry(task)->io_error = -EIO;
    task->state = TASK_COMPLETED;
    task_maybe_finish(task, task_entry(task)->cancel_requested);
  }
}

static void loop_drain_ready(io_loop_t *loop) {
  // Handlers may enqueue more (including themselves), so keep going until the
  // queue is empty.
  while (loop->ready_head != NULL) {
    io_task_t *task = loop->ready_head;
    ready_unlink(task);
    task_entry(task)->ready_queued = 0;
    if (task_entry(task)->removed) {
      continue;
    }
    if (task_entry(task)->cancel_requested || task->state == TASK_COMPLETED) {
      int cancelled = task_entry(task)->cancel_requested;
      task_maybe_finish(task, cancelled);
    } else {
      drive_task(task);
    }
  }
}

static void loop_reap(io_loop_t *loop) {
  io_task_entry_t *entry = loop->removed_head;
  loop->removed_head = NULL;
  while (entry != NULL) {
    io_task_entry_t *next = entry->removed_next;
    task_free(&entry->task);
    entry = next;
  }
}

io_loop_t *io_loop_new(io_backend_kind_t kind) {
  io_loop_t *loop = calloc(1, sizeof *loop);
  if (loop == NULL) {
    return NULL;
  }

  // Ordered candidates: explicit kind gets one shot; AUTO prefers the richer
  // proactor backend and falls back to epoll if the host lacks io_uring.
  const io_backend_t *candidates[2];
  int ncand = 0;
  switch (kind) {
  case IO_BACKEND_EPOLL:
    candidates[ncand++] = &epoll_backend;
    break;
  case IO_BACKEND_URING:
    candidates[ncand++] = &uring_backend;
    break;
  case IO_BACKEND_AUTO:
  default:
    candidates[ncand++] = &uring_backend;
    candidates[ncand++] = &epoll_backend;
    break;
  }

  for (int i = 0; i < ncand; i++) {
    loop->backend = candidates[i];
    if (loop->backend->init(loop) == 0) {
      tl_loop = loop;
      return loop;
    }
    loop->impl = NULL;
  }

  free(loop);
  return NULL;
}

static void io_loop_free(io_loop_t *loop) {
  if (loop == NULL) {
    return;
  }
  loop_reap(loop);
  loop_free_timers(loop);
  if (loop->backend != NULL && loop->backend->fini != NULL) {
    loop->backend->fini(loop);
  }
  if (tl_loop == loop) {
    tl_loop = NULL;
  }
  free(loop);
}

io_loop_t *io_loop_current(void) { return tl_loop; }

io_timer_t *io_timer_add(io_loop_t *loop, uint64_t delay_ms, void (*cb)(void *),
                         void *ctx) {
  if (loop == NULL || cb == NULL) {
    return NULL;
  }
  io_timer_t *timer = calloc(1, sizeof *timer);
  if (timer == NULL) {
    return NULL;
  }
  timer->loop = loop;
  timer->deadline_ms = engine_now_ms() + delay_ms;
  timer->cb = cb;
  timer->ctx = ctx;
  timer->index = SIZE_MAX;
  if (timer_insert(loop, timer) != 0) {
    free(timer);
    return NULL;
  }
  return timer;
}

void io_timer_cancel(io_timer_t *timer) {
  if (timer == NULL || timer->loop == NULL) {
    return;
  }
  timer_remove(timer->loop, timer);
  free(timer);
}

void io_loop_set_stop_handler(io_loop_t *loop, void (*cb)(io_loop_t *)) {
  if (loop == NULL) {
    return;
  }
  loop->stop_cb = cb;
}

void io_loop_request_stop(io_loop_t *loop) {
  if (loop == NULL) {
    return;
  }
  atomic_store_explicit(&loop->stop_requested, 1, memory_order_relaxed);
  // Without a stop handler there is no drain phase: stop means exit.
  if (loop->stop_cb == NULL) {
    atomic_store_explicit(&loop->exit_requested, 1, memory_order_relaxed);
  }
  if (loop->backend != NULL) {
    loop->backend->wake(loop);
  }
}

int io_loop_stop_requested(const io_loop_t *loop) {
  return loop != NULL &&
         atomic_load_explicit(&loop->stop_requested, memory_order_relaxed);
}

void io_loop_exit(io_loop_t *loop) {
  if (loop == NULL) {
    return;
  }
  atomic_store_explicit(&loop->exit_requested, 1, memory_order_relaxed);
  if (loop->backend != NULL) {
    loop->backend->wake(loop);
  }
}

const char *io_loop_backend_name(const io_loop_t *loop) {
  if (loop == NULL || loop->backend == NULL) {
    return "none";
  }
  return loop->backend->name;
}

int io_loop_add_task(io_loop_t *loop, io_task_t *task) {
  if (loop == NULL || task == NULL) {
    return -1;
  }
  int rc = loop->backend->attach(loop, task);
  if (rc != 0) {
    return rc;
  }
  io_task_entry_t *entry = task_entry(task);
  entry->attached = 1;
  entry->owner = loop;
  entry->tasks_prev = NULL;
  entry->next = loop->tasks;
  if (loop->tasks != NULL) {
    loop->tasks->tasks_prev = entry;
  }
  loop->tasks = entry;
  ready_enqueue_loop(loop, task);
  return 0;
}

static void loop_drain_shutdown(io_loop_t *loop) {
  io_task_t *batch[MAX_EVENTS];
  while (loop->tasks != NULL) {
    loop_reap(loop);
    if (loop->tasks == NULL) {
      break;
    }
    int n = loop->backend->wait(loop, batch, MAX_EVENTS, 10);
    for (int i = 0; i < n; i++) {
      drive_task(batch[i]);
    }
    loop_drain_ready(loop);
    loop->backend->flush(loop);
    loop_reap(loop);
  }
}

static void loop_shutdown_tasks(io_loop_t *loop) {
  io_task_entry_t *entry = loop->tasks;
  while (entry != NULL) {
    io_task_entry_t *next = entry->next;
    if (!entry->removed) {
      task_cancel(&entry->task);
    }
    entry = next;
  }
  loop_drain_shutdown(loop);
}

void io_loop_shutdown(io_loop_t *loop) {
  if (loop == NULL) {
    return;
  }
  loop_shutdown_tasks(loop);
  io_loop_free(loop);
}

void io_loop_run(io_loop_t *loop) {
  io_task_t *batch[MAX_EVENTS];
  loop_reap(loop);
  while (!atomic_load_explicit(&loop->exit_requested, memory_order_relaxed)) {
    int timeout_ms = loop_timeout_ms(loop, engine_now_ms());
    int n = loop->backend->wait(loop, batch, MAX_EVENTS, timeout_ms);
    loop_run_timers(loop, engine_now_ms());
    if (atomic_load_explicit(&loop->stop_requested, memory_order_relaxed) &&
        loop->stop_cb != NULL && !loop->stop_notified) {
      loop->stop_notified = 1;
      loop->stop_cb(loop);
      if (atomic_load_explicit(&loop->exit_requested, memory_order_relaxed)) {
        break;
      }
    }
    for (int i = 0; i < n; i++) {
      drive_task(batch[i]);
    }
    loop_drain_ready(loop);
    loop->backend->flush(loop);
    loop_reap(loop);
  }
}
