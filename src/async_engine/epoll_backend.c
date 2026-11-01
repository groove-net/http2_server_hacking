#include "engine_internal.h"
#include <sys/epoll.h>
#include <sys/eventfd.h>

// Epoll readiness backend. Mirrors the original hand-rolled engine exactly:
// edge-triggered fd registration, blocking epoll_wait, synchronous read/write
// performed by read_step()/write_step().
typedef struct {
  int epfd;
  int wake_fd;
} epoll_impl_t;

static int ep_init(io_loop_t *loop) {
  epoll_impl_t *im = calloc(1, sizeof *im);
  if (im == NULL) {
    return -1;
  }
  im->epfd = epoll_create1(0);
  im->wake_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (im->epfd < 0 || im->wake_fd < 0) {
    if (im->wake_fd >= 0) {
      close(im->wake_fd);
    }
    if (im->epfd >= 0) {
      close(im->epfd);
    }
    free(im);
    return -1;
  }

  // Wake channel: lets task_ready() interrupt epoll_wait when work is queued
  // outside of an fd event. data.ptr == NULL marks it as the wake sentinel.
  struct epoll_event ev;
  ev.events = EPOLLIN | EPOLLET;
  ev.data.ptr = NULL;
  if (epoll_ctl(im->epfd, EPOLL_CTL_ADD, im->wake_fd, &ev) != 0) {
    close(im->wake_fd);
    close(im->epfd);
    free(im);
    return -1;
  }

  loop->impl = im;
  return 0;
}

static void ep_fini(io_loop_t *loop) {
  epoll_impl_t *im = loop->impl;
  if (im == NULL) {
    return;
  }
  close(im->wake_fd);
  close(im->epfd);
  free(im);
  loop->impl = NULL;
}

static int ep_attach(io_loop_t *loop, io_task_t *task) {
  epoll_impl_t *im = loop->impl;
  struct epoll_event ev;
  ev.events = EPOLLET;
  ev.data.ptr = task;
  if (epoll_ctl(im->epfd, EPOLL_CTL_ADD, task->fd, &ev) != 0) {
    return -1;
  }
  task_entry(task)->read_watch_armed = 0;
  task_entry(task)->write_watch_armed = 0;
  return 0;
}

static int ep_read_watch(io_loop_t *loop, io_task_t *task, int enable) {
  epoll_impl_t *im = loop->impl;
  io_task_entry_t *entry = task_entry(task);
  if (task->fd < 0 || entry->read_watch_armed == enable) {
    return 0;
  }

  struct epoll_event ev;
  ev.events = EPOLLET | (enable ? EPOLLIN : 0) |
              (entry->write_watch_armed ? EPOLLOUT : 0);
  ev.data.ptr = task;
  if (epoll_ctl(im->epfd, EPOLL_CTL_MOD, task->fd, &ev) != 0) {
    return -1;
  }
  entry->read_watch_armed = enable;
  return 0;
}

static int ep_write_watch(io_loop_t *loop, io_task_t *task, int enable) {
  epoll_impl_t *im = loop->impl;
  io_task_entry_t *entry = task_entry(task);
  if (task->fd < 0 || entry->write_watch_armed == enable) {
    return 0;
  }

  struct epoll_event ev;
  ev.events = EPOLLET | (entry->read_watch_armed ? EPOLLIN : 0) |
              (enable ? EPOLLOUT : 0);
  ev.data.ptr = task;
  if (epoll_ctl(im->epfd, EPOLL_CTL_MOD, task->fd, &ev) != 0) {
    return -1;
  }
  entry->write_watch_armed = enable;
  return 0;
}

static int ep_park(io_loop_t *loop, io_task_t *task) {
  io_task_entry_t *entry = task_entry(task);
  int want_write = task->state == TASK_WAIT_WRITE ||
                   (task->state == TASK_ACTIVE && entry->write_blocked);
  return ep_write_watch(loop, task, want_write);
}

static int ep_detach(io_loop_t *loop, io_task_t *task) {
  epoll_impl_t *im = loop->impl;
  return epoll_ctl(im->epfd, EPOLL_CTL_DEL, task->fd, NULL);
}

static int ep_wait(io_loop_t *loop, io_task_t **out, int max, int timeout_ms) {
  epoll_impl_t *im = loop->impl;
  struct epoll_event events[MAX_EVENTS];
  int cap = max < MAX_EVENTS ? max : MAX_EVENTS;
  int n = epoll_wait(im->epfd, events, cap, timeout_ms);
  if (n <= 0) {
    return 0; // timeout or EINTR: let the loop process timers
  }
  int m = 0;
  for (int i = 0; i < n; i++) {
    io_task_t *task = (io_task_t *)events[i].data.ptr;
    if (task == NULL) { // wake sentinel: drain the eventfd counter
      uint64_t v;
      while (read(im->wake_fd, &v, sizeof v) == (ssize_t)sizeof v) {
      }
      continue;
    }
    unsigned evs = events[i].events;
    unsigned ready = 0;
    if ((evs & (EPOLLIN | EPOLLERR | EPOLLHUP)) != 0) {
      ready |= READY_READ;
    }
    if ((evs & (EPOLLOUT | EPOLLERR | EPOLLHUP)) != 0) {
      ready |= READY_WRITE;
    }
    task_entry(task)->ready_events = ready;
    out[m++] = task;
  }
  return m;
}

static void ep_flush(io_loop_t *loop) { (void)loop; }

static int ep_read_step(io_loop_t *loop, io_task_t *task) {
  (void)loop;
  if (task->read_buffer == NULL || task->read_size == 0) {
    return IO_STEP_AGAIN;
  }
  if (task->bytes_buffered == 0) {
    task->read_head = 0;
  }
  size_t span = task_read_free_span(task);
  if (span == 0) {
    return IO_STEP_AGAIN;
  }

  size_t tail = task_read_tail(task);
  ssize_t n;
  do {
    n = read(task->fd, task->read_buffer + tail, span);
  } while (n < 0 && errno == EINTR);
  if (n > 0) {
    task->bytes_buffered += (size_t)n;
    return IO_STEP_DONE;
  }
  if (n == 0) {
    return IO_STEP_EOF;
  }
  if (errno == EAGAIN || errno == EWOULDBLOCK) {
    return IO_STEP_AGAIN;
  }
  task_entry(task)->io_error = errno != 0 ? -errno : -EIO;
  return IO_STEP_ERROR;
}

static int ep_write_step(io_loop_t *loop, io_task_t *task) {
  (void)loop;
  if (!task->write_active || task->write_buffer == NULL ||
      task->write_off >= task->write_len ||
      task->write_len > task->write_size) {
    return IO_STEP_ERROR;
  }
  ssize_t n = write(task->fd, task->write_buffer + task->write_off,
                    task->write_len - task->write_off);
  if (n > 0) {
    task->write_off += (size_t)n;
    return IO_STEP_DONE;
  }
  if (n == 0) {
    return IO_STEP_AGAIN;
  }
  if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
    return IO_STEP_AGAIN;
  }
  task_entry(task)->io_error = errno != 0 ? -errno : -EIO;
  return IO_STEP_ERROR;
}

static void ep_wake(io_loop_t *loop) {
  epoll_impl_t *im = loop->impl;
  uint64_t one = 1;
  ssize_t r = write(im->wake_fd, &one, sizeof one);
  (void)r; // a full counter already means "wake pending"
}

static const io_backend_t epoll_backend = {
    .name = "epoll",
    .init = ep_init,
    .fini = ep_fini,
    .attach = ep_attach,
    .detach = ep_detach,
    .wait = ep_wait,
    .flush = ep_flush,
    .read_step = ep_read_step,
    .write_step = ep_write_step,
    .read_watch = ep_read_watch,
    .write_watch = ep_write_watch,
    .park = ep_park,
    .cancel = NULL, // synchronous steps: no in-flight op to cancel
    .wake = ep_wake,
};
