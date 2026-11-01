#include "engine_internal.h"
#include <liburing.h>
#include <linux/time_types.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>

// io_uring proactor backend.
//
// Mapping onto the engine's task/state-machine surface:
//   * Client data tasks (read_buffer != NULL) submit real async recv()/send()
//     operations via read_step()/write_step(); the kernel performs the copy and
//     we reap the completion.
//   * Readiness-only tasks (read_buffer == NULL, e.g. the listener) get a
//     multishot POLLIN, so their handler is driven on each readable event.
//   * A blocked write (or a TASK_WAIT_WRITE handler) arms a one-shot POLLOUT.
//   * task_ready() from another thread wakes the ring via an eventfd poll.
typedef struct {
  struct io_uring ring;
  int wake_fd;
  io_task_entry_t *cancel_head;
  io_task_entry_t *cancel_tail;
} uring_impl_t;

// user_data layout: task pointer (>= 8-byte aligned) in the high bits, op kind
// in the low 3 bits. A sentinel of UD_WAKE (task == NULL) marks the wake poll.
#define UD_KIND_MASK 7ULL
enum {
  UD_READ = 0,
  UD_WRITE = 1,
  UD_POLLIN = 2,
  UD_POLLOUT = 3,
  UD_WAKE = 4,
  UD_CANCEL = 5, // completion of an IORING_OP_POLL_REMOVE; no task
};

static struct io_uring_sqe *uring_sqe(io_loop_t *loop) {
  uring_impl_t *im = loop->impl;
  return io_uring_get_sqe(&im->ring);
}

static int uring_submit_read(io_loop_t *loop, io_task_t *task) {
  if (task->read_closed || task_entry(task)->read_inflight ||
      task->read_buffer == NULL || task->read_size == 0) {
    return -1;
  }
  if (task->bytes_buffered == 0) {
    task->read_head = 0;
  }
  size_t len = task_read_free_span(task);
  if (len == 0) {
    return -1;
  }
  struct io_uring_sqe *sqe = uring_sqe(loop);
  if (sqe == NULL) {
    return -1;
  }
  size_t tail = task_read_tail(task);
  io_uring_prep_recv(sqe, task->fd, task->read_buffer + tail, len, 0);
  sqe->user_data = (uint64_t)(uintptr_t)task | UD_READ;
  task_entry(task)->read_inflight = 1;
  task_entry(task)->inflight_ops++;
  return 0;
}

static int uring_submit_write(io_loop_t *loop, io_task_t *task) {
  if (!task->write_active || task->write_buffer == NULL ||
      task_entry(task)->write_inflight || task->write_off >= task->write_len ||
      task->write_len > task->write_size) {
    return -1;
  }
  struct io_uring_sqe *sqe = uring_sqe(loop);
  if (sqe == NULL) {
    return -1;
  }
  size_t len = task->write_len - task->write_off;
  io_uring_prep_send(sqe, task->fd, task->write_buffer + task->write_off, len,
                     0);
  sqe->user_data = (uint64_t)(uintptr_t)task | UD_WRITE;
  task_entry(task)->write_inflight = 1;
  task_entry(task)->inflight_ops++;
  return 0;
}

static int uring_init(io_loop_t *loop) {
  uring_impl_t *im = calloc(1, sizeof *im);
  if (im == NULL) {
    return -1;
  }
  if (io_uring_queue_init(256, &im->ring, 0) < 0) {
    free(im);
    return -1;
  }
  im->wake_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (im->wake_fd < 0) {
    io_uring_queue_exit(&im->ring);
    free(im);
    return -1;
  }

  // Wake channel: lets task_ready() interrupt a blocked wait() from another
  // thread. Multishot, so it never needs re-arming.
  struct io_uring_sqe *sqe = io_uring_get_sqe(&im->ring);
  if (sqe == NULL) {
    close(im->wake_fd);
    io_uring_queue_exit(&im->ring);
    free(im);
    return -1;
  }
  io_uring_prep_poll_multishot(sqe, im->wake_fd, POLLIN);
  sqe->user_data = UD_WAKE;
  if (io_uring_submit(&im->ring) < 0) {
    close(im->wake_fd);
    io_uring_queue_exit(&im->ring);
    free(im);
    return -1;
  }

  loop->impl = im;
  return 0;
}

static void uring_fini(io_loop_t *loop) {
  uring_impl_t *im = loop->impl;
  if (im == NULL) {
    return;
  }
  io_uring_queue_exit(&im->ring);
  close(im->wake_fd);
  free(im);
  loop->impl = NULL;
}

static int uring_attach(io_loop_t *loop, io_task_t *task) {
  (void)loop;
  (void)task;
  return 0;
}

static int uring_read_watch(io_loop_t *loop, io_task_t *task, int enable) {
  // Readiness-only tasks (no receive buffer) are driven purely by readability.
  if (!enable || task->read_buffer != NULL || task_entry(task)->watch_armed) {
    return 0;
  }
  struct io_uring_sqe *sqe = uring_sqe(loop);
  if (sqe == NULL) {
    return -1;
  }
  io_uring_prep_poll_multishot(sqe, task->fd, POLLIN);
  sqe->user_data = (uint64_t)(uintptr_t)task | UD_POLLIN;
  task_entry(task)->watch_armed = 1;
  task_entry(task)->inflight_ops++;
  return 0;
}

static int uring_detach(io_loop_t *loop, io_task_t *task) {
  // Normal teardown happens with no data op in flight: a completion is fully
  // consumed before its handler runs, and the next op is armed only afterwards.
  (void)loop;
  (void)task;
  return 0;
}

static void uring_flush(io_loop_t *loop) {
  uring_impl_t *im = loop->impl;
  io_uring_submit(&im->ring);
}

static int uring_out_add(io_task_t **out, int m, int max, io_task_t *task) {
  io_task_entry_t *entry = task_entry(task);
  if (entry->batch_queued) {
    return m;
  }
  if (m < max) {
    entry->batch_queued = 1;
    out[m++] = task;
  }
  return m;
}

static void uring_cancel_list_add(uring_impl_t *im, io_task_entry_t *entry) {
  entry->cancel_prev = im->cancel_tail;
  entry->cancel_next = NULL;
  if (im->cancel_tail != NULL) {
    im->cancel_tail->cancel_next = entry;
  } else {
    im->cancel_head = entry;
  }
  im->cancel_tail = entry;
}

static void uring_cancel_list_remove(uring_impl_t *im, io_task_entry_t *entry) {
  if (entry->cancel_prev != NULL) {
    entry->cancel_prev->cancel_next = entry->cancel_next;
  } else {
    im->cancel_head = entry->cancel_next;
  }
  if (entry->cancel_next != NULL) {
    entry->cancel_next->cancel_prev = entry->cancel_prev;
  } else {
    im->cancel_tail = entry->cancel_prev;
  }
  entry->cancel_prev = NULL;
  entry->cancel_next = NULL;
}

static int uring_retry_cancels(io_loop_t *loop) {
  uring_impl_t *im = loop->impl;
  int pending = 0;
  io_task_entry_t *entry = im->cancel_head;
  while (entry != NULL) {
    io_task_entry_t *next = entry->cancel_next;
    if (entry->task.read_buffer != NULL || !entry->watch_armed) {
      uring_cancel_list_remove(im, entry);
      entry->cancel_pending = 0;
      entry = next;
      continue;
    }
    struct io_uring_sqe *sqe = io_uring_get_sqe(&im->ring);
    if (sqe == NULL) {
      io_uring_submit(&im->ring);
      sqe = io_uring_get_sqe(&im->ring);
    }
    if (sqe == NULL) {
      pending = 1;
      entry = next;
      continue;
    }
    uint64_t target = (uint64_t)(uintptr_t)&entry->task | UD_POLLIN;
    io_uring_prep_poll_remove(sqe, target);
    sqe->user_data = UD_CANCEL;
    uring_cancel_list_remove(im, entry);
    entry->cancel_pending = 0;
    io_uring_submit(&im->ring);
    entry = next;
  }
  return pending;
}

static int uring_wait(io_loop_t *loop, io_task_t **out, int max,
                      int timeout_ms) {
  uring_impl_t *im = loop->impl;

  if (uring_retry_cancels(loop) && (timeout_ms < 0 || timeout_ms > 10)) {
    timeout_ms = 10;
  }

  struct __kernel_timespec ts;
  struct __kernel_timespec *tsp = NULL;
  if (timeout_ms >= 0) {
    ts.tv_sec = timeout_ms / 1000;
    ts.tv_nsec = (long)(timeout_ms % 1000) * 1000000L;
    tsp = &ts;
  }
  struct io_uring_cqe *first = NULL;
  int r = io_uring_submit_and_wait_timeout(&im->ring, &first, 1, tsp, NULL);
  if (r < 0 && r != -ETIME && r != -EINTR) {
    return 0;
  }

  int m = 0;
  unsigned head;
  unsigned reaped = 0;
  struct io_uring_cqe *cqe;
  io_uring_for_each_cqe(&im->ring, head, cqe) {
    reaped++;
    uint64_t ud = cqe->user_data;
    int res = cqe->res;

    if (ud == UD_WAKE) {
      uint64_t v;
      while (read(im->wake_fd, &v, sizeof v) == (ssize_t)sizeof v) {
      }
      continue;
    }
    if (ud == UD_CANCEL) {
      continue; // poll_remove acknowledgement: the target poll reports next
    }

    io_task_t *task = (io_task_t *)(uintptr_t)(ud & ~UD_KIND_MASK);
    int aborting = task_entry(task)->cancel_requested || task_entry(task)->io_abort ||
                   task->state == TASK_COMPLETED;

    switch (ud & UD_KIND_MASK) {
    case UD_READ:
      if (task_entry(task)->read_inflight) {
        task_entry(task)->read_inflight = 0;
        if (task_entry(task)->inflight_ops > 0) {
          task_entry(task)->inflight_ops--;
        }
      }
      if (aborting) {
        task_entry(task)->read_op_ready = 0;
        break;
      }
      if (res == -EAGAIN || res == -EINTR) {
        if (uring_submit_read(loop, task) == 0) {
          continue;
        }
        task_entry(task)->read_op_result = IO_STEP_ERROR;
        task_entry(task)->read_op_ready = 1;
      } else if (res > 0) {
        task->bytes_buffered += (size_t)res;
        // A proactor read can't observe "no more data right now", so process
        // this chunk immediately; the task re-arms a fresh read afterwards.
        task_entry(task)->read_op_result = IO_STEP_AGAIN;
        task_entry(task)->read_op_ready = 1;
      } else if (res == 0) {
        task_entry(task)->read_op_result = IO_STEP_EOF;
        task_entry(task)->read_op_ready = 1;
      } else {
        task_entry(task)->io_error = res < 0 ? res : -EIO;
        task_entry(task)->read_op_result = IO_STEP_ERROR;
        task_entry(task)->read_op_ready = 1;
      }
      break;
    case UD_WRITE:
      if (task_entry(task)->write_inflight) {
        task_entry(task)->write_inflight = 0;
        if (task_entry(task)->inflight_ops > 0) {
          task_entry(task)->inflight_ops--;
        }
      }
      if (aborting) {
        task_entry(task)->write_op_ready = 0;
        break;
      }
      if (res == -EAGAIN || res == -EINTR) {
        // Do not resubmit a blocked send: park on POLLOUT instead.
        task_entry(task)->write_op_result = IO_STEP_AGAIN;
        task_entry(task)->write_op_ready = 1;
      } else if (res >= 0) {
        task->write_off += (size_t)res;
        task_entry(task)->write_op_result = IO_STEP_DONE;
        task_entry(task)->write_op_ready = 1;
      } else {
        task_entry(task)->io_error = res < 0 ? res : -EIO;
        task_entry(task)->write_op_result = IO_STEP_ERROR;
        task_entry(task)->write_op_ready = 1;
      }
      break;
    case UD_POLLOUT:
      task_entry(task)->ready_events |= READY_WRITE;
      task_entry(task)->write_blocked = 0;
      if (task_entry(task)->watch_armed) {
        task_entry(task)->watch_armed = 0;
        if (task_entry(task)->inflight_ops > 0) {
          task_entry(task)->inflight_ops--;
        }
      }
      break;
    case UD_POLLIN:
      if ((cqe->flags & IORING_CQE_F_MORE) != 0) {
        task_entry(task)->watch_armed = 1;
        if (aborting) {
          continue; // wait for poll_remove's final completion
        }
      } else {
        if (task_entry(task)->watch_armed) {
          task_entry(task)->watch_armed = 0;
          if (task_entry(task)->cancel_pending) {
            uring_cancel_list_remove(im, task_entry(task));
            task_entry(task)->cancel_pending = 0;
          }
          if (task_entry(task)->inflight_ops > 0) {
            task_entry(task)->inflight_ops--;
          }
        }
      }
      break;
    default:
      break; // wake sentinel / unknown: no task
    }

    m = uring_out_add(out, m, max, task);
  }
  io_uring_cq_advance(&im->ring, reaped);
  return m;
}

static int uring_read_step(io_loop_t *loop, io_task_t *task) {
  if (uring_submit_read(loop, task) != 0) {
    task_entry(task)->io_error = errno != 0 ? -errno : -EIO;
    return IO_STEP_ERROR;
  }
  return IO_STEP_PENDING; // completion arrives through wait()
}

static int uring_write_step(io_loop_t *loop, io_task_t *task) {
  if (uring_submit_write(loop, task) != 0) {
    task_entry(task)->io_error = errno != 0 ? -errno : -EIO;
    return IO_STEP_ERROR;
  }
  return IO_STEP_PENDING;
}

static int uring_park(io_loop_t *loop, io_task_t *task) {
  if (task_entry(task)->watch_armed) {
    return 0;
  }
  int want_write = task->state == TASK_WAIT_WRITE ||
                   (task->state == TASK_ACTIVE && task_entry(task)->write_blocked);
  if (!want_write || task->fd < 0) {
    return 0;
  }
  struct io_uring_sqe *sqe = uring_sqe(loop);
  if (sqe == NULL) {
    return -1;
  }
  io_uring_prep_poll_add(sqe, task->fd, POLLOUT);
  sqe->user_data = (uint64_t)(uintptr_t)task | UD_POLLOUT;
  task_entry(task)->watch_armed = 1;
  task_entry(task)->inflight_ops++;
  return 0;
}

static void uring_cancel(io_loop_t *loop, io_task_t *task) {
  uring_impl_t *im = loop->impl;
  if (task->fd < 0) {
    return;
  }
  if (task->read_buffer == NULL && task_entry(task)->watch_armed) {
    // Listener: a multishot POLLIN is outstanding. Remove it by its user_data;
    // its completion still carries the task pointer, so the task is freed when
    // that CQE is reaped.
    uint64_t target = (uint64_t)(uintptr_t)task | UD_POLLIN;
    struct io_uring_sqe *sqe = uring_sqe(loop);
    if (sqe == NULL) {
      io_uring_submit(&im->ring); // flush queued SQEs and retry
      sqe = uring_sqe(loop);
    }
    if (sqe != NULL) {
      io_uring_prep_poll_remove(sqe, target);
      sqe->user_data = UD_CANCEL;
      io_uring_submit(&im->ring);
      task_entry(task)->cancel_pending = 0;
      return;
    }
    // No SQE available: retry from wait()/flush() once SQ space frees. Keep
    // watch_armed and inflight_ops set; the final poll CQE owns the task.
    if (!task_entry(task)->cancel_pending) {
      task_entry(task)->cancel_pending = 1;
      uring_cancel_list_add(im, task_entry(task));
    }
    return;
  }
  // Pending recv/send or a parked POLLOUT watch: shutdown wakes it without
  // closing the fd (task_finish still owns the close).
  shutdown(task->fd, SHUT_RDWR);
}

static void uring_wake(io_loop_t *loop) {
  uring_impl_t *im = loop->impl;
  uint64_t one = 1;
  ssize_t r = write(im->wake_fd, &one, sizeof one);
  (void)r;
}

static const io_backend_t uring_backend = {
    .name = "io_uring",
    .init = uring_init,
    .fini = uring_fini,
    .attach = uring_attach,
    .detach = uring_detach,
    .wait = uring_wait,
    .flush = uring_flush,
    .read_step = uring_read_step,
    .write_step = uring_write_step,
    .read_watch = uring_read_watch,
    .park = uring_park,
    .cancel = uring_cancel,
    .wake = uring_wake,
};
