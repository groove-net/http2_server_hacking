# Building an Async I/O Engine

This document reconstructs the async-engine, a stateful mechanism that allows a program to make progress on many I/O tasks concurrently on a single thread without blocking on any one of them.

## Layout

```
tcp_server_hacking/
├── include/
│   └── async_engine.h          # public contract
└── src/
    └── async_engine/
        ├── async_engine.c      # scheduler, state machine, lifetime
        ├── engine_internal.h   # backend seam and loop fields
        ├── epoll_backend.c     # readiness backend
        └── uring_backend.c     # proactor backend
```

The public header is the boundary. The scheduler owns task lifetime and policy;
the backends own epoll/io_uring. Phase 1 starts with everything in one file and
splits it apart in Phase 5.

The tutorial is intentionally incremental. Each phase leaves a working module,
then records the limitations that force the next phase.

## Phase 1 — One Loop, Many Tasks

Can one thread wait for multiple file descriptors and run application work for
whichever one becomes ready, without ever blocking on one descriptor?

The answer is an epoll loop with a registry of tasks. A task owns a file
descriptor and a handler. The loop waits for an event on a task descriptor,
dispatches the corresponding handler, and repeats.

### Create epoll system

First, we wrap epoll instance creation and deletion inside the library

```c
// include/async_engine.h
#ifndef ASYNC_ENGINE_H
#define ASYNC_ENGINE_H

typedef struct io_loop io_loop_t;

io_loop_t *io_loop_new(void);
void io_loop_free(io_loop_t *loop);

#endif /* ASYNC_ENGINE_H */
```

```c
// src/async_engine.c
#include "../../include/async_engine.h"

#include <errno.h>
#include <stdlib.h>
#include <sys/epoll.h>
#include <unistd.h>

struct io_loop {
  int epfd;
};

io_loop_t *io_loop_new(void) {
  io_loop_t *loop = calloc(1, sizeof(*loop));
  if (loop == NULL) {
    return NULL;
  }

  loop->epfd = epoll_create1(EPOLL_CLOEXEC);
  if (loop->epfd < 0) {
    free(loop);
    return NULL;
  }

  return loop;
}

void io_loop_free(io_loop_t *loop) {
  if (loop == NULL) {
    return;
  }

  close(loop->epfd);
  free(loop);
}
```

`epoll_create1(EPOLL_CLOEXEC)` creates the epoll instance and marks it
close-on-exec, so the descriptor is not leaked into child processes. The loop
does not own any application file descriptors yet.

At this point the loop can be created and destroyed, but it cannot do any work.

### Defining a task

A task is the unit of I/O-dependent work. Every task in the engine is represented by an `io_task` structure. At this stage it needs:

- the file descriptor to watch;
- the readiness events it cares about;
- a caller-owned context pointer;
- a handler that runs when the task is ready.

Add the event mask constants, the handler type, and the task type to the public header. The event mask is part of the public contract because callers choose whether they want read readiness, write readiness, or both.

```c
// include/async_engine.h
...
#include <stddef.h>

typedef struct io_task io_task_t;

enum {
  IO_READ  = 1u << 0,
  IO_WRITE = 1u << 1,
};

typedef void (*io_task_handler_t)(io_task_t *task, unsigned events);

struct io_task {
  int fd;
  void *args;
  io_task_handler_t handler;
};

```

#### Events

Readiness is a request from the caller. A task may only need to read, only need
to write, or care about both.

When the handler runs, `events` contains the translated readiness mask reported
by epoll. `IO_READ` means the descriptor is readable; the handler decides how
much to read. `IO_WRITE` means the descriptor is writable; the handler decides
what to write.

To bridge our public contract with the Linux kernel, we implement translation
and validation helpers in the `.c` file.

This implementation translates the public mask to epoll's mask:

```c
// src/async_engine.c
...
static uint32_t epoll_mask(unsigned events) {
  uint32_t mask = 0;

  if (events & IO_READ) {
    mask |= EPOLLIN;
  }
  if (events & IO_WRITE) {
    mask |= EPOLLOUT;
  }

  return mask;
}
```

This implementation converts epoll's mask back to the public mask:

```c
// src/async_engine.c
...
static unsigned ready_mask(uint32_t events) {
  unsigned mask = 0;

  if (events & EPOLLIN) {
    mask |= IO_READ;
  }
  if (events & EPOLLOUT) {
    mask |= IO_WRITE;
  }

  return mask;
}
```

This implementation validates the public mask inputted by the caller:

```c
// src/async_engine.c
...
static int valid_events(unsigned events) {
  return events != 0 && (events & ~(IO_READ | IO_WRITE)) == 0;
}
```

Errors and hang-ups are not translated into fake readability or writability.
If epoll reports only an error/hang-up, the engine treats the task as finished.
The handler's own `read()` or `write()` reports the actual error.

Once a task is attached, its handler checks which readiness event actually
occurred:

```c
// Example: Inside the task handler
static void handle_client(io_task_t *task, unsigned events) {
  // Check if the descriptor is ready to be read from
  if (events & IO_READ) {
    // task->fd is readable
  }

  // Check if the descriptor is ready to accept writes
  if (events & IO_WRITE) {
    // task->fd is writable
  }
}
```

#### args

`args` is a caller-owned pointer for passing custom context, such as connection
state or the loop itself. Handlers almost always need state that is not part of
the engine: a connection object, a parser, a callback, or a user pointer.

The engine never interprets `args`. It does not own it. The caller is
responsible for the lifetime of whatever `args` points to, and for cleaning it
up when the task is removed if that is appropriate. The engine only stores it
and makes it available to the handler through the task.

```c
// Example: Inside the task handler
static void handle_ex1(io_task_t *task, unsigned events) {
  example_t *i = task->args;
}
```

`example_t` can be an `int`, `char`, `float`, or custom struct.

A common arg is the live `io_loop_t` instance. A handler can then attach its own
new task to the system; that example appears in Attaching and Deleting Tasks.

### Task Registry

The public `io_task_t` struct is intentionally minimal—it only exposes what the caller and handler care about. However, the engine has its own bookkeeping requirements: it needs to track every active task in a linked list so it can clean them up during teardown. We hide these implementation details behind a private `io_task_entry_t` wrapper defined entirely inside the `.c` file:

```c
// src/async_engine.c
...
typedef struct io_task_entry {
  io_task_t task;
  struct io_task_entry *next;
} io_task_entry_t;

struct io_loop {
  int epfd;
  io_task_entry_t *tasks;
};
```

Because the task is embedded, the engine can recover the entry from the public
pointer:

```c
// src/async_engine.c
...
static io_task_entry_t *task_entry(io_task_t *task) {
  return (io_task_entry_t *)((char *)task - offsetof(io_task_entry_t, task));
}
```

`offsetof()` comes from `<stddef.h>`. This is the same idea as `container_of()`
in the Linux kernel, reduced to the one container we need.

The engine allocates the task entry, so creation and release belong here. Add
both functions to the public header:

```c
// include/async_engine.h
...
io_task_t *task_create(void);
void task_free(io_task_t *task);
```

Creation allocates and zeroes the private entry. `task_free()` closes
`task->fd` when it is non-negative, sets it to `-1`, and releases the entry.
There is no separate destructor:

```c
// src/async_engine.c
...
io_task_t *task_create(void) {
  io_task_entry_t *entry = calloc(1, sizeof(*entry));
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
```

`io_loop_free()` does not walk live tasks. Finish them first; the reaper below
releases removed entries and `task_free()` closes any fd that is still open.
Later phases make this destructor private behind `io_loop_shutdown()`.

The loop owns the task entry after a successful attach. The caller does not
free the `io_task_t *` it got from `task_create()`.

The loop can now own tasks, but it still needs a way to register them.

### Attaching and Deleting Tasks

A task cannot watch itself; it must be registered with the loop. Creation is
already defined in Task Registry; attachment registers an existing task with a
loop.

Add attachment to the header:

```c
// include/async_engine.h
...
int io_loop_add_task(io_loop_t *loop, io_task_t *task, unsigned events);
```

Let's break down what each attachment parameter requires:

- **`loop`**: The stateful engine instance that will own and monitor this task.
- **`task`**: The task to attach. Its `fd`, `args`, and `handler` must already be set.
- **`events`**: The readiness mask defined in Defining a task. It says whether to watch for reads, writes, or both.

A caller registers a socket for both reading and writing like this:

```c
// Example: Creating and attaching a task with multiple event flags
io_task_t *task = task_create();
if (task == NULL) {
  // Handle allocation failure
}
task->fd = client_fd;
task->args = conn_state;
task->handler = handle_client;
if (io_loop_add_task(loop, task, IO_READ | IO_WRITE) != 0) {
  task_free(task); // closes client_fd and releases the task
}
```

A common `args` value is the live `io_loop_t` instance. A handler can then add
its own new task to the system. In the example below, an example task handler
retrieves `io_loop_t` from `task->args` and registers a new task during
dispatch:

```c
// Example: Inside the task handler
static void handle_ex2(io_task_t *task, unsigned events) {
  io_loop_t *loop = task->args;
  ...
  io_task_t *next = task_create();
  if (next != NULL) {
    next->fd = pair[1];
    next->handler = print_once;
    if (io_loop_add_task(loop, next, IO_READ) != 0) {
      task_free(next); // closes pair[1]
    }
  }
}
```

`io_loop_add_task` updates the registry and the epoll set before the handler returns; the next `epoll_wait` sees the new fd. No locking is needed, and the new task is not part of the current event batch.

Now let's look at attachment. It validates the caller-owned fields and installs
the descriptor into epoll:

```c
// src/async_engine.c
...
int io_loop_add_task(io_loop_t *loop, io_task_t *task, unsigned events) {
  if (loop == NULL || task == NULL || task->fd < 0 || task->handler == NULL ||
      !valid_events(events)) {
    errno = EINVAL;
    return -1;
  }

  io_task_entry_t *entry = task_entry(task);
  struct epoll_event ev = {
      .events = epoll_mask(events),
      .data.ptr = entry,
  };

  if (epoll_ctl(loop->epfd, EPOLL_CTL_ADD, task->fd, &ev) < 0) {
    return -1;
  }

  entry->next = loop->tasks;
  loop->tasks = entry;

  return 0;
}
```

Take note of this in particular: `ev.data.ptr = entry`. epoll stores a pointer to the
private registry entry, not the file descriptor.

The pointer returned by `task_create()` points at `entry.task`, not at the
beginning of the allocation. That is an interior pointer. The caller must not
`free(task)` and must not assume the task can be copied or relocated.

Attachment has one ownership rule:

- on success, the loop owns the task entry and the task owns its fd;
- on failure, `task_free()` closes the fd and releases the entry.

Once `task->fd` is set, never close it manually. `task_free()` owns that
close.

#### Deleting a task safely

Deleting a task means closing its descriptor and releasing its entry. The first
instinct is to do that immediately:

```c
// src/async_engine.c
...
void task_cancel(io_task_t *task) {
  task_free(task);
}
```

This has two problems.

1. The task may still be referenced. `epoll_wait` returns a batch of events. A
handler for event A can remove task B, and task B can have an event later in
the same batch. Freeing B leaves that later `data.ptr` dangling. A handler can
also remove itself while the dispatcher still holds a pointer to it. ASan turns
the second case into an immediate error.
2. `task_free()` releases the entry, but it does not unlink it from
`loop->tasks`. The registry now points at freed memory. It also cannot revise the
event batch that `epoll_wait()` already returned, so later events for that task
are still in flight.

These types of bugs are not rare; they are a normal consequence of storing
pointers in kernel events.

The fix is to mark first, then reap. `task_cancel()` only marks the task; the
reaper calls `task_free()`, which closes the fd and releases the entry. The
private entry gains a `removed` tombstone flag.

```c
// src/async_engine.c
...
typedef struct io_task_entry {
  ...
  int removed;
} io_task_entry_t;

```

The initial state of `removed` should be `0` when a new task is created. `calloc()` already initializes `removed` to `0`. This means that the task is still alive.

`task_cancel()` only sets the private flag:

```c
// src/async_engine.c
...
void task_cancel(io_task_t *task) {
  if (task == NULL) {
    return;
  }

  io_task_entry_t *entry = task_entry(task);
  if (entry->removed) {
    return;
  }

  entry->removed = 1;
}
```

Cancellation is idempotent. The task pointer must not be used for any other
purpose after cancellation is requested. The reaper closes the descriptor and
releases the entry after the event batch through `task_free()`.

Now we can write a private function, we’ll call it `loop_reap`, that walks the task list, and for only tasks marked as removed, it calls `task_free` to release the task entry from the registry:

```c
// src/async_engine.c
...
static void loop_reap(io_loop_t *loop) {
  io_task_entry_t **link = &loop->tasks;

  while (*link != NULL) {
    io_task_entry_t *entry = *link;

    if (!entry->removed) {
      link = &entry->next;
      continue;
    }

    *link = entry->next;

    task_free(&entry->task);
  }
}
```

`task_free()` closes the descriptor, which removes it from the epoll set.
Freeing the entry here, after the batch, means a task cancelled by one handler
cannot free itself before another event in the same batch reaches the
dispatcher.

With attachment and deletion defined, the loop can wait for events and reclaim
removed entries at safe batch boundaries.

### Run the loop

Add the run function to the header:

```c
// include/async_engine.h
...
int io_loop_run(io_loop_t *loop);
```

`io_loop_run()` waits for events, translates the kernel mask, and calls the
handler with the ready mask. The handler performs the actual I/O.

`io_loop_run` now frees tombstoned tasks only at a batch boundary using `loop_reap`. `loop_reap` is called at the top of each iteration and after each batch, and the loop skips a task whose tombstone is already set:

```c
// src/async_engine.c
...
int io_loop_run(io_loop_t *loop) {
  if (loop == NULL) {
    errno = EINVAL;
    return -1;
  }

  struct epoll_event events[16];

  while (loop->tasks != NULL) {
    loop_reap(loop); // Reap tombstoned tasks

    if (loop->tasks == NULL) {
      break;
    }

    int n = epoll_wait(loop->epfd, events, 16, -1);

    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      return -1;
    }

    for (int i = 0; i < n; i++) {
      io_task_entry_t *entry = events[i].data.ptr;

      if (entry->removed) {
        continue;
      }

      unsigned ready = ready_mask(events[i].events);

      if (ready == 0) {
        task_cancel(&entry->task);
        continue;
      }

      entry->task.handler(&entry->task, ready);
    }

    loop_reap(loop); // Reap tombstoned tasks
  }

  return 0;
}
```

The loop runs until the registry is empty. There is no stop flag and no
shutdown API yet. If a task is never cancelled, the loop runs forever.

`epoll_wait()` with a timeout of `-1` blocks until at least one event or a
signal. `EINTR` is retried; other errors are returned to the caller.

The handler receives the event mask and performs its own I/O. If it reads, it
must not block the worker. It can use a bounded read, `MSG_DONTWAIT`, or its own
nonblocking setup. Phase 1 does not set `O_NONBLOCK`; that is one of the
limitations this phase exposes.

The loop can now dispatch tasks and reclaim removed entries at safe batch
boundaries.

### Phase 1 module

The header has grown, but only by what the implementation earned:

```c
#ifndef ASYNC_ENGINE_H
#define ASYNC_ENGINE_H

#include <stddef.h>
#include <stdint.h>

typedef struct io_loop io_loop_t;
typedef struct io_task io_task_t;

enum {
  IO_READ  = 1u << 0,
  IO_WRITE = 1u << 1,
};

typedef void (*io_task_handler_t)(io_task_t *task, unsigned events);

struct io_task {
  int fd;
  void *args;
  io_task_handler_t handler;
};

io_loop_t *io_loop_new(void);
void io_loop_free(io_loop_t *loop);

int io_loop_run(io_loop_t *loop);

io_task_t *task_create(void);
void task_free(io_task_t *task);

int io_loop_add_task(io_loop_t *loop, io_task_t *task, unsigned events);

void task_cancel(io_task_t *task);

#endif
```

### Checkpoint

A minimal test can create a pipe or socket pair, create and attach the read end
with a handler that owns a local buffer, write bytes to the other end, and verify
that the handler sees them. Cancelling the task from the handler should terminate
the loop cleanly once the registry is empty.

## Phase 2 — Handling Reads Properly

So far, when a descriptor is readable, the engine invokes the handler with information about what event occurred. Users of our engine build their own handlers to perform application logic. The handlers inspect the `events` parameter to see what happened (such as `IO_READ`) and access the `io_task_t` task instance that triggered it via the `task` parameter. However, the user still has to write manual code to perform the raw `read()`, manage buffers, and handle partial reads without blocking.

Phase 2 moves the read logic **into the engine**. We want the engine to drive
non-blocking reads and track partial data for the user. The caller still owns the
storage: the task carries a `read_buffer` and its capacity, and the engine fills
that buffer whenever the descriptor becomes readable.

Phase 1 passed the epoll mask to the handler because the handler performed the
I/O and needed to know which direction was ready. In Phase 2 the engine owns the
read step and fills `read_buffer` before invoking the handler, so the handler
only needs the task. Replace the Phase 1 event callback with a process callback
and a small state machine:

```c
// include/async_engine.h
...
typedef enum {
  TASK_ACTIVE,     /* the engine may attempt a read */
  TASK_PROCESSING, /* run the application handler */
  TASK_COMPLETED   /* remove the task */
} task_state_t;

typedef void (*io_task_process_t)(io_task_t *task);

struct io_task {
  int fd;
  task_state_t state;

  uint8_t *read_buffer; /* caller-owned ring storage */
  size_t read_size;     /* capacity of read_buffer */
  size_t read_head;     /* first unconsumed byte */
  size_t bytes_buffered;/* bytes currently stored in the ring */

  void *args;
  io_task_process_t process_handler;
};
```

`TASK_ACTIVE` means the engine owns the next I/O step. `TASK_PROCESSING` means
hand the accumulated data to the application. `TASK_COMPLETED` means the task is
finished and the reaper may release it.

Let's break down what each new field represents:

- **`read_buffer`**: Caller-owned ring storage where incoming bytes are accumulated.
- **`read_size`**: The capacity of `read_buffer`, protecting the engine from buffer overflows.
- **`read_head`**: The first unconsumed byte. The handler consumes from here.
- **`bytes_buffered`**: How many bytes are currently stored in the ring.

The engine appends at the wrapped tail, `(read_head + bytes_buffered) % read_size`.
The valid bytes are therefore up to two spans:

```c
size_t span = task_read_span(task);       /* bytes before the wrap */
uint8_t *first = task->read_buffer + task->read_head;
uint8_t *second = task->read_buffer;      /* valid when span < bytes_buffered */
```

`task_read_consume(task, n)` advances `read_head` and reduces `bytes_buffered`.
One leftover byte no longer blocks reads while the ring has free space.

### Reading the ring from the handler

The handler does not read from index `0`. It starts at `read_head` and handles at
most two spans:

```c
static void on_read(io_task_t *task) {
  size_t buffered = task->bytes_buffered;
  size_t span = task_read_span(task);
  const uint8_t *first = task->read_buffer + task->read_head;
  const uint8_t *second = task->read_buffer; /* valid when span < buffered */
  size_t second_len = buffered - span;

  /* Parse first[0..span), then second[0..second_len) if needed. */

  task_read_consume(task, buffered); /* or consume only what was parsed */
  task->state = TASK_ACTIVE;
}
```

`task_read_consume()` updates the ring bookkeeping for the handler. Do not
decrement `bytes_buffered` or advance `read_head` manually.

### Updating the Registration API

The engine is a pipe, not an allocator. It does not allocate, grow, or free the
read buffer. `io_loop_add_task()` only binds the task; it registers no readiness
interest and does not require read storage yet. The caller installs the ring with
`task_setup_read()` when ready, then calls `task_start_read()`.

```c
// include/async_engine.h
...
int io_loop_add_task(io_loop_t *loop, io_task_t *task);
int task_setup_read(io_task_t *task, uint8_t *read_buffer, size_t read_size);
int task_start_read(io_task_t *task);
```

Attachment installs the descriptor without read or write interest:

```c
// src/async_engine.c
...
static int task_update_read_watch(io_task_t *task, int enable);

int io_loop_add_task(io_loop_t *loop, io_task_t *task) {
  if (loop == NULL || task == NULL || task->fd < 0 ||
      task->process_handler == NULL) {
    errno = EINVAL;
    return -1;
  }

  io_task_entry_t *entry = task_entry(task);
  entry->task.bytes_buffered = 0;

  struct epoll_event ev = {
      .events = EPOLLET,
      .data.ptr = entry,
  };

  if (epoll_ctl(loop->epfd, EPOLL_CTL_ADD, task->fd, &ev) < 0) {
    return -1;
  }

  entry->next = loop->tasks;
  loop->tasks = entry;

  return 0;
}

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
  if (task == NULL || task->state == TASK_COMPLETED) {
    return -EPIPE;
  }
  io_task_entry_t *entry = task_entry(task);
  entry->read_stopped = 0;
  task->read_closed = 0;
  entry->read_eof_pending = 0;
  if (task->read_buffer == NULL) {
    task->state = TASK_PROCESSING; // readiness-only task
  }
  return task_update_read_watch(task, 1);
}

void task_stop_read(io_task_t *task) {
  if (task == NULL || task->state == TASK_COMPLETED) {
    return;
  }
  task_entry(task)->read_stopped = 1;
  task_update_read_watch(task, 0);
}
```

These use a private `read_stopped` bit on the entry. `task_stop_read()` disarms
read interest without closing the read side; `task_start_read()` clears the bit
and can be called again to restart reads.

Now that the engine assumes responsibility for reading from the descriptor, it must ensure the file descriptor is set to non-blocking mode—regardless of how the caller configured it.

To do this safely without breaking caller-provided settings, we implement two helper functions. One to set to non blocking mode and another to restore the file status flags to whatever the caller had configured:

```c
// src/async_engine.c
...
#include <fcntl.h>

static int set_nonblocking(int fd, int *old_flags) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0) {
    return -1;
  }

  *old_flags = flags;

  if ((flags & O_NONBLOCK) == 0) {
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
      return -1;
    }
  }

  return 0;
}

static void restore_flags(int fd, int flags) {
  if (flags >= 0) {
    fcntl(fd, F_SETFL, flags);
  }
}
```

`F_SETFL` must preserve the existing flags and add `O_NONBLOCK`. Assigning only
`O_NONBLOCK` would discard existing file-status flags.

We now integrate these helpers into `io_loop_add_task()`. We invoke `set_nonblocking()` before installing the descriptor, and restore the original flags if `epoll_ctl()` fails:

```c
// src/async_engine.c
...
int io_loop_add_task(io_loop_t *loop, io_task_t *task) {
  if (loop == NULL || task == NULL || task->fd < 0 ||
      task->process_handler == NULL || task->read_buffer == NULL ||
      task->read_size == 0) {
    errno = EINVAL;
    return -1;
  }

  io_task_entry_t *entry = task_entry(task);
  entry->task.bytes_buffered = 0;

  int old_flags = -1;
  if (set_nonblocking(task->fd, &old_flags) < 0) {
    return -1;
  }

  struct epoll_event ev = {
      .events = EPOLLET,
      .data.ptr = entry,
  };

  if (epoll_ctl(loop->epfd, EPOLL_CTL_ADD, task->fd, &ev) < 0) {
    restore_flags(task->fd, old_flags); // Restore flags on epoll_ctl failure
    return -1;
  }

  entry->next = loop->tasks;
  loop->tasks = entry;

  return 0;
}
```

The file descriptor is now non-blocking even if the caller supplied a blocking one. Ownership still transfers only on success. If attachment fails after the flag change, the original flags are restored.

### Task release

Phase 1's `task_cancel()` marks the task and the reaper calls `task_free()`,
which closes the descriptor and releases the engine entry without touching
`read_buffer`. The caller owns that storage, so there is no destructor change
in this phase.

### The switch to edge-triggered epoll

By default, an epoll system is level-triggered. Phase 1 uses level-triggered epoll. Level-triggered mode reports readiness as long as a descriptor has unread data. If a handler triggered by READ readiness does completely read all the bytes available in the descriptor before returning and leaves bytes behind, epoll will wake up and yell at you again on the next iteration of the `io_loop_run` loop. As long as there is *even one byte* left unread in the socket buffer, `epoll_wait()` will keep waking up and yelling at you on every single loop iteration.

If you have a massive data ready to be read in the descriptor buffer and you only read a portion of it, epoll will constantly interrupt your CPU to remind you about the rest. In high-concurrency system, this causes massive CPU waste and redundant wakeups (often called an epoll storm).

To build a high-performance engine, Phase 2 switches to edge-triggered mode (`EPOLLET`).

```c
// src/async_engine.c
...
static int task_update_read_watch(io_task_t *task, int enable) {
  if (tl_loop == NULL) {
    return -1;
  }

  struct epoll_event ev = {
      .events = EPOLLET | (enable ? EPOLLIN : 0),
      .data.ptr = task_entry(task),
  };

  return epoll_ctl(tl_loop->epfd, EPOLL_CTL_MOD, task->fd, &ev);
}
```

This function is the single switch for read readiness. The fd is already in the
epoll set because `io_loop_add_task()` registered it with `EPOLLET`; this helper
only changes whether `EPOLLIN` is part of that registration.

- `enable != 0` subscribes: `EPOLLIN` is added, so readable edges are delivered.
- `enable == 0` unsubscribes: `EPOLLIN` is removed, so the task stops receiving
  read readiness. The fd stays registered, and `EPOLLERR`/`EPOLLHUP` can still
  be reported.
- `EPOLL_CTL_MOD` updates the existing registration; it does not add or remove
  the fd, and it does not close it.
- `data.ptr` remains the task entry, so events still identify the task.
- `EPOLLET` stays set in both states; only the read-interest bit changes.

`task_start_read()` calls this with `enable = 1`, and `task_stop_read()` calls it
with `enable = 0`. That is how the engine subscribes and unsubscribes from read
readiness without changing the fd or destroying the task. Starting again after a
stop re-arms `EPOLLIN`; data already waiting in the socket is reported when the
interest is added again.

With edge-triggered mode, a readiness notification is delivered *only* when the descriptor changes state. When the descriptor is readable, you get one notification and that it.

Because we configured our descriptors with edge-triggered epoll (`EPOLLET`), we cannot afford to read just once and leave data in the kernel buffer. We must **drain** the descriptor completely during each notification cycle. This means that the engine must consume bytes from the descriptor until it hits `EAGAIN`, fills the buffer, or encounters EOF/error. If you fail to drain the descriptor completely, you will never receive another notification for the data still sitting in the kernel buffer.

To satisfy this requirement and automatically manage buffers for the caller, we
implement a single non-blocking read step and drive it from `drive_task()`
until the descriptor is drained or the buffer is full. The loop lives in
`drive_task()`, not inside the read function.

### **Implementing the Read Operation**

The engine needs a function that performs one non-blocking `read()`—pulling bytes
from the descriptor into our task buffer—when the epoll loop reports that a
descriptor is readable.

Each invocation can produce one of four outcomes:

- **Progress**
  Bytes were read and appended to the buffer.
- **No more data is currently available**
  The kernel buffer is empty for now, returning `EAGAIN` or `EWOULDBLOCK`,
  meaning we must pause and wait for the next epoll notification.
- **The task descriptor has been closed unexpectedly**
  A read of `0` bytes signals EOF.
- **The read failed**
  A system-level error occurred.

A full buffer is not a syscall outcome. We check capacity before calling the
step, so the result does not need a `FULL` state.

Use the same step vocabulary from the start, because writes will reuse it:

```c
// src/async_engine.c
...
enum {
  IO_STEP_DONE,   /* made progress */
  IO_STEP_AGAIN,  /* no progress right now */
  IO_STEP_EOF,    /* read returned 0 */
  IO_STEP_ERROR,  /* syscall failed */
};
```

Now let’s build the step. It performs one `read()` into the free span at the
ring tail and returns the outcome:

```c
// src/async_engine.c
...
static size_t task_read_tail(const io_task_t *task) {
  return (task->read_head + task->bytes_buffered) % task->read_size;
}

static size_t task_read_free_span(const io_task_t *task) {
  size_t tail = task_read_tail(task);
  size_t free_total = task->read_size - task->bytes_buffered;
  size_t contiguous = task->read_size - tail;
  return free_total < contiguous ? free_total : contiguous;
}

static int task_read_step(io_task_t *task) {
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

  return IO_STEP_ERROR;
}
```

### Driving the Read Operation

`drive_task()` is a small state machine. `IO_STEP_DONE` keeps the task in
`TASK_ACTIVE` and `continue` runs the read step again; there is no separate
inner drain loop:

```c
// src/async_engine.c
...
static void drive_task(io_task_t *task) {
  while (1) {
    switch (task->state) {
    case TASK_ACTIVE: {
      int st = task_read_step(task);

      if (st == IO_STEP_DONE) {
        continue; /* keep draining until EAGAIN, EOF, error, or a full buffer */
      }
      if (st == IO_STEP_ERROR) {
        task->state = TASK_COMPLETED;
        continue;
      }
      if (st == IO_STEP_EOF) {
        if (task->bytes_buffered > 0) {
          task->state = TASK_PROCESSING;
          continue;
        }
        task->state = TASK_COMPLETED;
        continue;
      }
      if (st == IO_STEP_AGAIN && task->bytes_buffered > 0) {
        task->state = TASK_PROCESSING;
        continue;
      }

      return; /* IO_STEP_AGAIN with no data: wait for another edge */
    }

    case TASK_PROCESSING:
      if (task->process_handler != NULL) {
        task->process_handler(task);
      } else {
        task->state = TASK_COMPLETED;
      }

      /* The handler may finish the task; otherwise the engine reads again. */
      if (task->state == TASK_PROCESSING) {
        if (task->bytes_buffered > 0) {
          return; /* residual input: wait for another edge */
        }
        task->state = TASK_ACTIVE;
      }
      continue;

    case TASK_COMPLETED:
      task_entry(task)->removed = 1;
      return;
    }
  }
}
```

The event loop hands each ready entry to `drive_task()` without passing an event
mask:

```c
for (int i = 0; i < n; i++) {
  io_task_entry_t *entry = events[i].data.ptr;

  if (entry->removed) {
    continue;
  }

  unsigned ready = ready_mask(events[i].events);
  if (ready == 0) {
    task_cancel(&entry->task);
    continue;
  }

  drive_task(&entry->task);
}
```

`ready == 0` means only `EPOLLERR`/`EPOLLHUP` was reported, so the task is
finished rather than readable or writable.

We use the outcome of the read step to decide what to do next:

- **`IO_STEP_DONE`:** stay in `TASK_ACTIVE`; `continue` runs the step again.
- **`IO_STEP_AGAIN` with partial data:** move to `TASK_PROCESSING`.
- **`IO_STEP_AGAIN` with zero bytes:** do not run the handler; wait for another
  edge.
- **`IO_STEP_EOF` with zero bytes:** move to `TASK_COMPLETED`.
- **`IO_STEP_EOF` with partial data:** move to `TASK_PROCESSING`; after the
  handler, the next active pass observes EOF with zero bytes and completes.
- **`IO_STEP_ERROR`:** move to `TASK_COMPLETED`; the handler is not called.

### Solidifying the mental model

To see how the event loop, `drive_task()`, and `task_read_step()` cooperate
under edge-triggered epoll (`EPOLLET`), let's trace a complete execution
lifecycle from kernel wakeup to handler invocation.

The engine's execution flows seamlessly from the event loop down to the user’s
task handler. When `io_loop_run()` blocks on `epoll_wait()`, it remains suspended
until the kernel signals a state change on a monitored file descriptor. Because
our descriptors operate in edge-triggered mode (`EPOLLET`), the kernel wakes up
the loop exactly once when new data becomes available.

Upon receiving this notification, `drive_task()` repeatedly calls
`task_read_step()`, pulling bytes into `task->read_buffer` via non-blocking
`read()` calls until the buffer reaches capacity, the data source is completely
drained (`EAGAIN`), or an end-of-file/error condition is met. This ensures that
every edge-triggered wakeup completely empties the kernel's available bytes
before the engine ever invokes the application handler.

Here are some concrete examples.

**Example 1:** Suppose a descriptor has **300 bytes** available in the kernel
buffer, and our task was registered with a buffer size of **1,024 bytes**.

- `drive_task()` calls `task_read_step()`.
- It successfully pulls all 300 bytes into the buffer. `bytes_buffered` becomes 300.
- The step returns `IO_STEP_DONE`, so `drive_task()` calls it again.
- The next `read()` finds nothing left and returns `-1` with `errno == EAGAIN`.
- The step returns `IO_STEP_AGAIN`, so the drain loop stops.
- `drive_task()` checks `bytes_buffered`. Since it is greater than 0, it moves the task to `TASK_PROCESSING`; the handler sees 300 bytes.
- Because the dispatcher kept calling the step until `EAGAIN`, the kernel buffer is now completely empty. Even though we didn't fill the 1,024-byte buffer, we successfully satisfied edge-triggered requirements by draining the source.

**Example 2:** Suppose a descriptor has **2,000 bytes** available, but our task
buffer size is only **1,024 bytes**.

- `drive_task()` calls `task_read_step()` repeatedly.
- It reads until `bytes_buffered` reaches 1,024.
- The next call sees `task->bytes_buffered >= task->read_size` and returns
  `IO_STEP_AGAIN` without issuing another `read()`.
- `drive_task()` moves the task to `TASK_PROCESSING`; the handler sees the fully
  populated 1,024-byte buffer.
- The handler returns. If it consumed the 1,024 bytes, the engine sets
  `TASK_ACTIVE` again and reads the remaining 976 bytes in the same drive, then
  runs the handler again. A full buffer alone does not strand kernel data.
- *Note:* Consumed bytes are removed from the ring head. In Phase 2, leaving
  bytes unconsumed parks the task until another edge; that edge can read into
  the free part of the ring. Phase 4 adds `read_paused` so reads can continue
  without automatically re-entering the handler.

### How the handler must operate

The handler never calls `read()` and never manages the ring tail. When it runs,
the engine has already placed bytes in `task->read_buffer`; read them from
`read_head`, using `task_read_span(task)` to know where the ring wraps. The data
may be a complete message or only part of one.

The handler consumes what it processed with `task_read_consume(task, n)`. It
must not simply decrement `bytes_buffered`, because consumption happens from the
head. When the ring becomes empty, the engine resets `read_head` to `0`.

If the handler consumes everything and wants more input, leaving the task in
`TASK_PROCESSING` makes the engine set `TASK_ACTIVE` and read again. If it is
finished with the descriptor, it sets `task->state = TASK_COMPLETED`.

If the handler leaves bytes unconsumed, the engine may still read into the free
part of the ring. It only pauses when the ring is full, or when the handler has
requested an explicit resume with `task_ready()`.

Consumption advances the ring head:

```c
// src/async_engine.c
...
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
```

### Phase 2 module

The public contract now includes the buffer and target:

```c
#ifndef ASYNC_ENGINE_H
#define ASYNC_ENGINE_H

#include <stddef.h>
#include <stdint.h>

typedef struct io_loop io_loop_t;
typedef struct io_task io_task_t;

typedef enum {
  TASK_ACTIVE,
  TASK_PROCESSING,
  TASK_COMPLETED
} task_state_t;

typedef void (*io_task_process_t)(io_task_t *task);

struct io_task {
  int fd;
  task_state_t state;

  uint8_t *read_buffer; /* caller-owned ring */
  size_t read_size;
  size_t read_head;
  size_t bytes_buffered;

  void *args;
  io_task_process_t process_handler;
};

io_loop_t *io_loop_new(void);
void io_loop_free(io_loop_t *loop);

int io_loop_run(io_loop_t *loop);

io_task_t *task_create(void);
void task_free(io_task_t *task);

static inline size_t task_read_span(const io_task_t *task) {
  if (task == NULL || task->read_size == 0) {
    return 0;
  }
  size_t span = task->read_size - task->read_head;
  return span < task->bytes_buffered ? span : task->bytes_buffered;
}

int task_read_consume(io_task_t *task, size_t n);
int task_setup_read(io_task_t *task, uint8_t *read_buffer, size_t read_size);
int task_start_read(io_task_t *task);
void task_stop_read(io_task_t *task);

int io_loop_add_task(io_loop_t *loop, io_task_t *task);

void task_cancel(io_task_t *task);

#endif
```

### Checkpoint

A useful Phase 2 test uses a socket pair:

1. Create a task, install a caller-owned 16-byte `read_buffer` with
   `task_setup_read()`, add it to the loop, then call `task_start_read()`.
2. Send 4 bytes, then 4 more, then 8 more.
3. Verify `process_handler` runs after partial reads and again with 16 bytes.
4. Send more than the target and verify a full buffer causes another handler
   pass without waiting for a new edge.
5. Close the peer and verify the EOF path: no callback with zero bytes, one
callback with partial bytes, then task completion.

---

# Phase 3 — Duplex I/O and a Real Write Path

Phase 2 assumes the task only reads. Once a handler has a response, it needs to
say:

> Write this. If the socket is full, keep reading and resume the write later.

That requires independent read and write progress. A task is one connection, so
it stays one task; it must not be split into a reader task and a writer task.
epoll registers a file descriptor once, and both directions belong to the same
connection.

Phase 2 already moved buffer ownership to the caller, so this phase does not
change who allocates memory. It adds a second buffer contract: the caller owns
both sides, and the engine only advances offsets and watches for readiness.

## Why a single read buffer is not enough

The tempting first implementation reuses the read buffer for the response:

```c
/* Do not do this while bytes_buffered still holds input. */
memcpy(task->read_buffer, response, response_len);
write(task->fd, task->read_buffer, response_len);
```

It fails as soon as the peer is still sending while the response is pending.
The read buffer owns unread input; the write side owns a different byte range.
Overwriting one with the other corrupts both. Separate storage also makes
partial writes explicit: the engine advances a write offset while the read side
keeps filling a different buffer.

Add the write-side fields to `io_task`:

```c
uint8_t *write_buffer; /* borrowed active write */
size_t write_size;     /* capacity of write_buffer */
size_t write_len;      /* bytes to send in this write request */
size_t write_off;      /* bytes accepted by write() so far */
int write_active;      /* engine owns this write request until write_done */
```

Each field has a distinct job:

- **`write_buffer`**: borrowed pointer to the caller's storage for the active
  write request. The engine does not copy or free it. It is `NULL` when no write
  is active.
- **`write_size`**: capacity of `write_buffer`. `task_write()` sets it to `len`;
  the write step rejects a request whose `write_len` exceeds it.
- **`write_len`**: total bytes this write request must send. It stays fixed for
  the life of the request.
- **`write_off`**: bytes already accepted by `write()` for this request. The
  engine advances it; the caller must not modify it. The request is complete
  when `write_off == write_len`.
- **`write_active`**: nonzero while the engine owns an active write request. It
  prevents a second `task_write()` from starting another request and tells the
  state machine to drive the write side.

`write_done` is not part of this group; it is the completion signal introduced
with the lifecycle flags below.

The engine never frees either buffer. It is a pipe, not an allocator. The caller
owns both and decides when their storage is reusable.

The application pointer also gets its final name in this phase: `args` becomes
`context`. It is still opaque to the engine.

## Starting a write

The public write surface has one call:

```c
int task_write(io_task_t *task, const uint8_t *buf, size_t len);
```

`task_write()` starts exactly one borrowed write request. It does not copy `buf`;
the caller must keep those bytes valid and unchanged until `write_done`. Only one
write request may be active at a time, so a second call returns `-EALREADY`.
It returns `-EPIPE` when the task is completing, `-EBUSY` when the task is in
`TASK_WAIT_WRITE`, and `-EINVAL` for bad arguments, a missing worker, or a task
owned by a different loop.

The engine only advances `write_off`. It never rewrites or frees the borrowed
buffer. Finishing the task is an explicit handler decision.

## Lifecycle flags

The write fields describe the active request. `write()` may accept all, part, or
none of it (`EAGAIN`), and `write_active` remains `1` in all three cases.

`write_done` is the per-request completion latch: it is set when the active
request reaches `write_len`, and the caller may reuse that buffer.

The two ends of the connection need the same kind of explicit signal. A read of
`0` is not “no data right now”; it is EOF or TCP half-close, and it is
permanent. If the engine only looked at `bytes_buffered`, an empty buffer and an
EOF would look identical, so it latches `read_closed` when the peer closes its
send side. `read_closed` is an event and can be cleared by `task_start_read()`
for descriptors whose EOF can be transient.

These are not extra state-machine states. They are edge latches, and each
answers a question the engine cannot answer from the buffers or the fd alone.
The public events are:

```c
int write_done;   /* the active write finished */
int read_closed;  /* EOF was observed on the read side */
```

- `write_done` is **per write request**. The handler clears it, recycles that
  buffer, and may start the next request.
- `read_closed` is an EOF observation. The handler decides whether to finish,
  pause, or re-arm reading.

The active write must remain valid and unchanged until `write_done`. A later
write request may reuse the same storage only after the handler has processed
that completion.

The read side can be paused before EOF. `task_stop_read()` is idempotent: it
disarms read interest and leaves already-buffered bytes for the handler. The
write side continues normally. `task_start_read()` re-arms interest and clears a prior EOF/stop state, so descriptors whose EOF or stop can be transient can become readable again.


## Extending the Phase 2 state machine

Phase 2 already replaced the event callback with `process_handler(task)` and
introduced `TASK_ACTIVE`, `TASK_PROCESSING`, and `TASK_COMPLETED`. Phase 3 does
not change that contract. It uses the same `TASK_ACTIVE` state to drive reads and
active writes, and `TASK_PROCESSING` to run the handler after either side makes
progress.

`TASK_READY` arrives in Phase 4. `TASK_WAIT_WRITE` arrives with handler-owned
output in Phase 6.

Phase 2 already moved normal completion from `task_cancel()` to
`TASK_COMPLETED`. Phase 4 adds explicit cancellation for tasks that are not
runnable.

The application handler is now allowed to:

- consume from `read_head` using `task_read_span(task)` for the wrap boundary;
  call `task_read_consume(task, n)` for the bytes it processed;
- call `task_write()` with a response;
- clear `write_done` and start the next write request;
- stop calling `task_write()` after the last request; completion is explicit;
- set `TASK_COMPLETED` for an early close;
- return to `TASK_ACTIVE` to let the engine continue.

Leaving unread bytes in the ring is allowed. Phase 2 parks the task until the
next edge; that edge reads into the free part of the ring. Phase 4 adds
`read_paused` so reads can continue without automatically re-entering the
handler, and `task_ready()` to request an explicit pass over the residual.

## One read step, one write step

Phase 2 already defined `IO_STEP_DONE`, `IO_STEP_AGAIN`, `IO_STEP_EOF`, and
`IO_STEP_ERROR` for reads. The same vocabulary works for writes, so Phase 3 does
not rename or replace it.

`IO_STEP_DONE` means “this step made progress,” not “the whole operation is
finished.” For a read, it means bytes were appended to the buffer. For a write,
it means bytes were accepted or the active write completed.

The read step fills the caller's read buffer:

```c
static int task_read_step(io_task_t *task) {
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
  return IO_STEP_ERROR;
}
```

The write step reads the borrowed active write and validates its length:

```c
static int task_write_step(io_task_t *task) {
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
  return IO_STEP_ERROR;
}
```

The socket must be non-blocking. Phase 2's attach path already enforces that.

## Writable interest

`task_write()` cannot rely on the descriptor already being watched for writing.
The engine tries the write immediately; only when that attempt returns `EAGAIN`
does it arm `EPOLLOUT`. When the request finishes, it removes the interest
again. Phase 3 stores the current loop in a worker-local pointer, which the
scheduling phases will reuse:

```c
static _Thread_local io_loop_t *tl_loop = NULL;
```

`io_loop_new()` publishes the loop after its epoll instance is created.

The interest helper reuses the readiness mask from attachment and toggles only
the write bit:

```c
static int task_update_write_watch(io_task_t *task, int enable) {
  if (tl_loop == NULL) {
    return -1;
  }

  struct epoll_event ev = {
      .events = EPOLLET | EPOLLIN | (enable ? EPOLLOUT : 0),
      .data.ptr = task_entry(task),
  };

  return epoll_ctl(tl_loop->epfd, EPOLL_CTL_MOD, task->fd, &ev);
}
```

`task_write()` records the active write and switches the task to `TASK_ACTIVE`.
It does not wait for writability:

```c
int task_write(io_task_t *task, const uint8_t *buf, size_t len) {
  if (task == NULL || buf == NULL || len == 0 || tl_loop == NULL) {
    return -EINVAL;
  }
  io_task_entry_t *entry = task_entry(task);
  if (entry->cancel_requested || task->state == TASK_COMPLETED) {
    return -EPIPE;
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
```

The engine tries the write immediately after the handler returns because the
socket is usually writable and waiting for an event would add latency. If the
write completes, no write interest was ever needed. If it returns `EAGAIN`, the
send buffer is full; the park path arms `EPOLLOUT`, and the engine waits for the
kernel to report writability before trying again.

## Keep engine bookkeeping private

The public task now has both buffers and lifecycle flags. One piece of duplex
state is still engine-owned: whether EOF has been delivered to the handler.

Extend the existing private entry; do not move the public fields:

```c
typedef struct io_task_entry {
  io_task_t task;
  struct io_task_entry *next;
  int removed;
  int read_eof_pending;
} io_task_entry_t;
```

`read_eof_pending` is set when a read returns `IO_STEP_EOF`. It ensures the
handler sees EOF even if there are zero bytes to process, and it is cleared when
the handler runs. Phase 4 adds the residual-input guard together with
`task_ready()`.

Later phases add their own private fields here. The public header stays focused
on what the caller sets or observes.

## Drive both directions

Phase 2's `drive_task(task)` only drained reads and then called the handler.
Phase 3 expands that same state machine to drive reads, writes, and handler
completion.

The core loop has one important ordering rule: deliver buffered input to the
handler before reading more, and report a completed write before starting the
next one.

```c
static void drive_task(io_task_t *task) {
  io_task_entry_t *entry = task_entry(task);

  while (1) {
    switch (task->state) {
    case TASK_ACTIVE: {
      if (task->bytes_buffered > 0) {
        task->state = TASK_PROCESSING;
        continue;
      }
      if (entry->read_eof_pending) {
        task->state = TASK_PROCESSING;
        continue;
      }
      if (task->write_done) {
        task->state = TASK_PROCESSING;
        continue;
      }

      if (!task->read_closed && !task_entry(task)->read_stopped &&
          task->read_buffer != NULL &&
          task->bytes_buffered < task->read_size) {
        int st = task_read_step(task);

        if (st == IO_STEP_DONE) {
          continue; /* keep draining until EAGAIN or the buffer is full */
        }
        if (st == IO_STEP_EOF) {
          task->read_closed = 1;
          entry->read_eof_pending = 1;
          task->state = TASK_PROCESSING;
          continue;
        }
        if (st == IO_STEP_ERROR) {
          task->state = TASK_COMPLETED;
          continue;
        }
        if (st == IO_STEP_AGAIN && task->bytes_buffered > 0) {
          task->state = TASK_PROCESSING;
          continue;
        }
      }

      if (task->write_active) {
        if (task->write_off >= task->write_len) {
          task_complete_write(task);
          task->state = TASK_PROCESSING;
          continue;
        }

        int st = task_write_step(task);
        if (st == IO_STEP_DONE) {
          if (task->write_off >= task->write_len) {
            task_complete_write(task);
            task->state = TASK_PROCESSING;
          }
          continue;
        }
        if (st == IO_STEP_ERROR) {
          task->state = TASK_COMPLETED;
          continue;
        }

        task_update_write_watch(task, 1);
        return; /* EAGAIN: parked until the fd is writable */
      }

      task_update_write_watch(task, 0);
      return; /* parked: wait for another fd event */
    }

    case TASK_PROCESSING: {
      entry->read_eof_pending = 0;

      if (task->process_handler) {
        task->process_handler(task);
      } else {
        task->state = TASK_COMPLETED;
      }

      if (task->state == TASK_ACTIVE) {
        if (task->bytes_buffered > 0) {
          return; /* residual input: park until another edge */
        }
        continue;
      }
      if (task->state == TASK_COMPLETED) {
        continue;
      }

      /* Unknown handler state: fail closed. */
      task->state = TASK_COMPLETED;
      continue;
    }

    case TASK_COMPLETED:
      task_entry(task)->removed = 1;
      return;
    }
  }
}
```

`task_complete_write()` is deliberately tiny:

```c
static void task_complete_write(io_task_t *task) {
  task->write_active = 0;
  task->write_done = 1;
  task_entry(task)->write_blocked = 0;
  task_update_write_watch(task, 0);
}
```

The handler owns the decision to start the next write request. The engine owns
only the active write.

The handler decides when to start the next write. `write_done` reports that the
one active buffer is reusable.

The event loop now hands the task directly to `drive_task()`. A completed task
marks itself removed and lets the existing reap path free it after the batch.

Because the engine always has the fd's current state, it can attempt a
non-blocking read or write without needing the exact event mask. `ready` is no
longer part of the task handler contract.

## Completion is explicit

The engine no longer decides when a task is finished. A read EOF is an event,
not an automatic task completion. The handler must either:

- set `task->state = TASK_COMPLETED`, or
- call `task_cancel(task)`.

This keeps the engine generic: a socket EOF, FIFO writer close, terminal
Ctrl-D, or seekable-file EOF can have different lifecycle meaning. The
application decides when the task is actually done.

## The first duplex task

## The first duplex task

A handler can look like this:

```c
static void on_process(io_task_t *task) {
  if (task->bytes_buffered > 0) {
    size_t n = task->bytes_buffered;
    size_t span = task_read_span(task);
    uint8_t *p = task->read_buffer + task->read_head;
    /* Parse p[0..span); if span < n, continue at task->read_buffer[0]. */
    task_read_consume(task, n);
  }

  if (task->write_done) {
    task->write_done = 0;
    /* The active write is reusable now. Start the next request if any. */
  }

  if (!task->write_active) {
    task_write(task, response, response_len);
  }

  if (task->read_closed && !task->write_active &&
      task->bytes_buffered == 0) {
    task->state = TASK_COMPLETED;
    return;
  }
  task->state = TASK_ACTIVE;
}
```

The handler owns delivery policy and the caller queue; the engine owns the one
active write and the lifetime of the task.

## No engine data queue

There is intentionally no write queue inside the engine. A queue would need to
copy bytes, or it would need a lifetime contract for arbitrary queued pointers,
or it would block the handler on memory allocation. All three are policy.

The caller queues. A practical pattern is one active slot plus one pending slot:

```c
typedef struct {
  const uint8_t *active;
  size_t active_len;
  const uint8_t *pending;
  size_t pending_len;
  int pending_used;
} conn_queue_t;
```

Call `task_write()` only when `write_active` is false. If it returns `-EALREADY`,
leave the pending buffer in place and return. On `write_done`, clear the active
slot and try the pending one again. If the caller queue is full, return
`-EAGAIN` to its producer.

Byte limits, rate limits, priority, and drop policy remain caller-side decisions.

## Phase 3 pain log

1. **No explicit scheduling.** A task can only resume from an fd event, so a
   handler with residual input or queued output has no way to ask for a pass.
2. **No cancellation.** There is no general cancel contract or cancel callback.
3. **No backend seam.** The scheduler still calls `read()`/`write()` and
   `epoll_ctl()` directly.
4. **No proactor.** Every operation is still assumed to complete synchronously.
5. **No timers.** A parked task can live forever.
6. **No wake across threads.** A producer cannot safely hand work to a worker.
7. **The queue lives above the engine.** The application must enforce
   backpressure, byte limits, rate policy, and drop policy.
8. **Fatal I/O errors are not separately reported.** This version completes the
   task; the application can log or attach error detail to its context.
9. **One buffer per direction is the caller's responsibility.** The engine does
   not allocate, grow, or free either one.

The immediate pain is scheduling. A task must be able to run because the
application asked it to, not only because a descriptor woke up.

---

# Phase 4 — Let Tasks Resume and Cancel

The next requirements come from real handlers:

- a parser consumed one message and has another buffered;
- a producer has queued output but the socket is quiet;
- a cleanup step must run after the current work;
- a cancellation request must not be lost while a handler is executing.

These are scheduler concerns.

## Add a ready queue

Add a second way for a task to become runnable, independent of fd readiness:

These fields live in the private entry:

```c
io_task_t *ready_prev;
io_task_t *ready_next;
int ready_queued;
int cancel_requested;
```

The loop owns the queue:

```c
struct io_loop {
  io_task_t *ready_head;
  io_task_t *ready_tail;
};
```

Each loop is worker-local, so the queue needs no lock. `task_ready()` may be
called from a handler, including the currently running task, but it must be
called on the worker thread that owns the task.

Add one state:

```c
typedef enum {
  TASK_ACTIVE,
  TASK_READY,      /* explicitly scheduled */
  TASK_PROCESSING,
  TASK_COMPLETED
} task_state_t;
```

`TASK_READY` is not a replacement for `TASK_ACTIVE`. `TASK_READY` means “run the
handler next; do not perform I/O first.” `TASK_PROCESSING` is the post-read or
post-write entry to the same handler.

Task construction does not change. Phase 1's `task_create()` and
`io_loop_add_task()` contract remains the only registration path. At this stage add
still only registers the fd; Phase 5 adds the wake channel and makes attach
schedule the first drive.

## Queue a task explicitly

The queue is doubly linked so a finished task can be removed without scanning:

```c
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
```

```c
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

// Wake the engine without changing what the task is currently doing.
static void task_kick(io_task_t *task) {
  if (task == NULL || tl_loop == NULL || task_entry(task)->cancel_requested ||
      task->state == TASK_COMPLETED) {
    return;
  }
  ready_enqueue(task);
}

void task_ready(io_task_t *task) {
  if (task == NULL) {
    return;
  }
  task->state = TASK_READY;
  ready_enqueue(task);
}
```

Reading starts explicitly; attach only binds the fd:

```c
static int task_update_read_watch(io_task_t *task, int enable);

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

void task_stop_read(io_task_t *task) {
  if (task == NULL || task->state == TASK_COMPLETED) {
    return;
  }
  task_entry(task)->read_stopped = 1;
  task_update_read_watch(task, 0);
}


```

`io_loop_current()` needs a worker-local pointer:

```c
static _Thread_local io_loop_t *tl_loop = NULL;

io_loop_t *io_loop_current(void) {
  return tl_loop;
}
```

`io_loop_new()` publishes the loop to the calling thread. Tasks remain pinned to
one worker.

## Resume residual input

Phase 3 parked a task whenever the handler left `bytes_buffered > 0`. The ready
queue now gives that residual a way back without waiting for another fd edge. Add
the guard to the private entry:

```c
typedef struct io_task_entry {
  ...
  int read_paused;
} io_task_entry_t;
```

`read_paused` now prevents automatic handler re-entry, but it must not stop the
engine from filling the free part of the ring:

```c
if (task->bytes_buffered == 0) {
  entry->read_paused = 0;
}
if (task->bytes_buffered > 0 && !entry->read_paused) {
  task->state = TASK_PROCESSING;
  continue;
}
...
if (!task->read_closed && task->read_buffer != NULL &&
    task->bytes_buffered < task->read_size) {
  int st = task_read_step(task);
  if (st == IO_STEP_DONE) {
    entry->read_paused = 0; /* new input may unblock the handler */
    continue;
  }
  ...
}
```

When the handler returns with data still buffered, mark the handler paused:

```c
if (task->state == TASK_ACTIVE) {
  if (task->bytes_buffered > 0) {
    entry->read_paused = 1;
  }
  ...
  continue;
}
```

The engine keeps reading into free ring space. If new data arrives, it clears
`read_paused` and runs the handler again. If the ring is full and there is no
new edge, the handler calls `task_ready(task)` to request an explicit pass over
the residual.

## Drain the ready queue

`task_finish()` is defined below, once completion and cancellation share a
release path:

```c
static void task_finish(io_task_t *task, int cancelled);
```

```c
static void loop_drain_ready(io_loop_t *loop) {
  while (loop->ready_head != NULL) {
    io_task_t *task = loop->ready_head;

    ready_unlink(task);
    task_entry(task)->ready_queued = 0;

    if (task_entry(task)->cancel_requested || task->state == TASK_COMPLETED) {
      task_finish(task, task_entry(task)->cancel_requested);
    } else {
      drive_task(task);
    }
  }
}
```

The drain must continue until the queue is empty. A handler may queue itself or
another task.

Call it after each event batch, before the tombstone reaper:

```c
for (int i = 0; i < n; i++) {
  io_task_entry_t *entry = events[i].data.ptr;

  if (!entry->removed) {
    drive_task(&entry->task);
  }
}

loop_drain_ready(loop);
loop_reap(loop);
```

`ready_enqueue_loop()` does not wake the backend. `task_ready()` is a
worker-thread operation, so the loop is already running or about to run.
Cross-thread shutdown requests still use the backend wake hook.

## Make `TASK_READY` real

```c
case TASK_READY:
case TASK_PROCESSING: {
  task_entry(task)->read_eof_pending = 0;
  task_entry(task)->read_paused = 0;

  if (task->process_handler != NULL) {
    task->process_handler(task);
  } else {
    task->state = TASK_COMPLETED;
  }

  if (task_entry(task)->cancel_requested) {
    task->state = TASK_COMPLETED;
  }
  continue;
}
```

Now a handler can request a second pass without an fd event:

```c
task_ready(task);
return; /* leave the task alive */
```

The loop runs it again after the current batch.

## Track the running task

Cancellation must distinguish “this task is executing right now” from “this task
is parked.” Keep a worker-local pointer and wrap the state-machine body:

```c
static _Thread_local io_task_t *tl_current = NULL;

static void drive_task_impl(io_task_t *task);

static void drive_task(io_task_t *task) {
  io_task_t *prev_current = tl_current;
  tl_current = task;

  if (task_entry(task)->cancel_requested ||
      task->state == TASK_COMPLETED) {
    int cancelled = task_entry(task)->cancel_requested;
    tl_current = prev_current;
    task_finish(task, cancelled);
    return;
  }

  drive_task_impl(task);

  if (task->state == TASK_COMPLETED) {
    int cancelled = task_entry(task)->cancel_requested;
    tl_current = prev_current;
    task_finish(task, cancelled);
    return;
  }

  tl_current = prev_current;
}
```

Rename Phase 3's existing `drive_task()` body to `drive_task_impl()`. The wrapper
restores `tl_current` after every path through the state machine.

## Cancellation

A task may need to stop while it is parked in `TASK_ACTIVE`, `TASK_READY`, or
`TASK_PROCESSING`. Add a flag and the two completion callbacks:

```c
int cancel_requested;

void (*on_complete)(void *context, int status, uint8_t *read_buffer);
void (*on_cancel)(void *context);
```

`task_cancel()` has two cases:

- the task is currently executing;
- the task is not currently executing.

```c
void task_cancel(io_task_t *task) {
  if (task == NULL || task->state == TASK_COMPLETED) {
    return;
  }
  if (task_entry(task)->cancel_requested) {
    return;
  }

  task_entry(task)->cancel_requested = 1;

  if (task == tl_current) {
    return; /* drive_task() finishes it when the handler returns */
  }
  task_finish(task, 1);
}
```

The public predicate is read-only and tolerates `NULL`:

```c
int is_task_cancel_requested(const io_task_t *task) {
  return task != NULL && task_entry_const(task)->cancel_requested;
}
```

The current-task case is essential: a handler may cancel itself, but it cannot
free the task while its own stack frame still uses it.

Completion and cancellation share one release path. The task is marked removed
rather than freed immediately, so the Phase 2 reaper can release it after the
current event batch:

```c
static void task_finish(io_task_t *task, int cancelled) {
  io_task_entry_t *entry = task_entry(task);

  if (entry->removed) {
    return;
  }
  entry->removed = 1;

  ready_unlink(task);

  if (cancelled && task->on_cancel != NULL) {
    task->on_cancel(task->context);
  } else if (task->on_complete != NULL) {
    task->on_complete(task->context,
                      cancelled ? ECANCELED : entry->io_error,
                      task->read_buffer);
  }
}
```

The reaper closes the descriptor and releases the entry through
`task_free()`. Buffers remain caller-owned; the callback may free them, the
engine does not.

Normal completion uses the same path. After `drive_task_impl()` returns, the
`drive_task()` wrapper checks `TASK_COMPLETED` and calls `task_finish()`.

A task can complete normally or because cancellation was requested while its
handler was running. Both cases release through `task_finish()`.

## Phase 4 pain log

1. **`task_ready()` cannot wake a blocked wait.** It queues work but does not
   interrupt `epoll_wait()`.
2. **Cancellation has no in-flight-operation story.** A proactor will change
   that.
3. **The loop still cannot stop.** There is no stop flag and no timeout.
4. **epoll remains hard-coded.**
5. **Worker lifetime is implicit.**
6. **No timers.**
7. **No backend capability check.**
8. **Cross-thread scheduling is not supported by design.**

Now there is enough pressure to introduce a backend seam.

---

# Phase 5 — Pull epoll Behind a Seam

Move the epoll-specific operations out of the scheduler one group at a time:

1. creating and destroying the epoll instance;
2. attaching and detaching descriptors;
3. waiting for events;
4. reading and writing;
5. waking a blocked wait.

Then define the interface.

## Build on the stable setup

Task construction and registration are already separate by Phase 1:

- the caller obtains a task from `task_create()` and fills its public fields;
- `io_loop_add_task()` registers that task with a loop;
- read storage is installed separately with `task_setup_read()`;
- the caller owns the buffers, while the engine owns the task and fd after a
  successful add.

Phase 5 does not repeat that setup. It only moves the epoll-specific mechanics
behind the interface below, so a second backend can be added without changing
the task API.

## The backend interface

```c
typedef struct io_backend {
  const char *name;

  int (*init)(struct io_loop *loop);
  void (*fini)(struct io_loop *loop);

  int (*attach)(struct io_loop *loop, io_task_t *task);
  int (*detach)(struct io_loop *loop, io_task_t *task);

  int (*wait)(struct io_loop *loop, io_task_t **out, int max, int timeout_ms);
  void (*flush)(struct io_loop *loop);

  int (*read_step)(struct io_loop *loop, io_task_t *task);
  int (*write_step)(struct io_loop *loop, io_task_t *task);

  int (*write_watch)(struct io_loop *loop, io_task_t *task, int enable);

  void (*park)(struct io_loop *loop, io_task_t *task);
  void (*cancel)(struct io_loop *loop, io_task_t *task);
  void (*wake)(struct io_loop *loop);
} io_backend_t;
```

`io_loop_t` becomes opaque to users:

```c
struct io_loop {
  const io_backend_t *backend;
  void *impl;

  io_task_t *ready_head;
  io_task_t *ready_tail;
  io_task_entry_t *tasks;
  io_task_entry_t *removed_head;

  void (*stop_cb)(struct io_loop *loop);
  int stop_notified;
  _Atomic int stop_requested;
  _Atomic int exit_requested;

  io_timer_t **timers;
  size_t timer_count;
  size_t timer_cap;
};
```

The public header exposes `io_loop_t`, `io_task_t`, and the functions. The
vtable stays in `engine_internal.h`.

Attachment dispatches through the backend and schedules the task's first run:

```c
int io_loop_add_task(io_loop_t *loop, io_task_t *task) {
  if (loop == NULL || task == NULL) {
    return -1;
  }

  int rc = loop->backend->attach(loop, task);
  if (rc != 0) {
    return rc;
  }

  io_task_entry_t *entry = task_entry(task);
  entry->next = loop->tasks;
  loop->tasks = entry;
  ready_enqueue_loop(loop, task);
  return 0;
}
```

Detach is not public. `task_finish()` calls `loop->backend->detach()` before the
reaper closes the fd. The batch-safety problem from Phase 1 is still solved by
the `removed` tombstone and the reaper, not by detach. There is no pause/reattach
API, so exposing detach would let callers tear down registration without owning
the task's lifetime.

## Move the read and write steps

The scheduler stops calling `read()` and `write()` directly:

```c
case TASK_ACTIVE: {
  int st = loop->backend->read_step(loop, task);
  /* ... */
  st = loop->backend->write_step(loop, task);
  /* ... */
}
```

The epoll backend is a move of the old code with the new fields:

```c
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
  return IO_STEP_ERROR;
}
```

```c
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
  return IO_STEP_ERROR;
}
```

### Complete the epoll backend

The backend needs the lifecycle and registration functions that used to live in
the loop. This is the epoll half of the seam:

```c
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

  struct epoll_event ev;
  ev.events = EPOLLIN | EPOLLET;
  ev.data.ptr = NULL; // wake sentinel
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

static void ep_park(io_loop_t *loop, io_task_t *task) {
  io_task_entry_t *entry = task_entry(task);
  int want_write = task->state == TASK_WAIT_WRITE ||
                   (task->state == TASK_ACTIVE && entry->write_blocked);
  ep_write_watch(loop, task, want_write);
}

static int ep_detach(io_loop_t *loop, io_task_t *task) {
  epoll_impl_t *im = loop->impl;
  return epoll_ctl(im->epfd, EPOLL_CTL_DEL, task->fd, NULL);
}

static int ep_wait(io_loop_t *loop, io_task_t **out, int max,
                   int timeout_ms) {
  epoll_impl_t *im = loop->impl;
  struct epoll_event events[MAX_EVENTS];
  int cap = max < MAX_EVENTS ? max : MAX_EVENTS;
  int n = epoll_wait(im->epfd, events, cap, timeout_ms);
  if (n <= 0) {
    return 0; // timeout or EINTR: let the loop run its tick
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

static void ep_wake(io_loop_t *loop) {
  epoll_impl_t *im = loop->impl;
  uint64_t one = 1;
  ssize_t r = write(im->wake_fd, &one, sizeof one);
  (void)r; // a full counter already means "wake pending"
}

```

`ep_wait` is completed below with the wake sentinel and readiness translation.

The read side owns `read_buffer`, `read_size`, and `bytes_buffered`. The write
side owns `write_buffer`, `write_size`, `write_len`, and `write_off`. They never
alias by accident.

## Preserve readiness

Edge-triggered epoll needs the event mask. The backend stores it on the task:

```c
static int ep_wait(io_loop_t *loop, io_task_t **out, int max,
                   int timeout_ms) {
  epoll_impl_t *im = loop->impl;
  struct epoll_event events[MAX_EVENTS];
  int cap = max < MAX_EVENTS ? max : MAX_EVENTS;
  int n = epoll_wait(im->epfd, events, cap, timeout_ms);
  if (n <= 0) {
    return 0; // timeout or EINTR: let the loop run its tick
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
      ready |= IO_READ;
    }
    if ((evs & (EPOLLOUT | EPOLLERR | EPOLLHUP)) != 0) {
      ready |= IO_WRITE;
    }

    task_entry(task)->ready_events = ready;
    out[m++] = task;
  }
  return m;
}
```

```c
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
    .cancel = NULL,
    .wake = ep_wake,
};
```

`TASK_ACTIVE` does not need the bits for its own read/write steps. Phase 6 uses
them for handler-owned output, where a writable event must invoke the handler
rather than the engine's write step.

With the seam in place, the write-interest helper delegates to the backend
instead of touching epoll directly:

```c
static int task_update_write_watch(io_task_t *task, int enable) {
  io_loop_t *loop = tl_loop;
  if (loop != NULL && loop->backend != NULL &&
      loop->backend->write_watch != NULL) {
    return loop->backend->write_watch(loop, task, enable);
  }
  return 0;
}
```

It is called when a write step returns `EAGAIN`, and with `enable = 0` from
`task_complete_write()`.

The read side mirrors it:

```c
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
```



## The wake channel

`task_ready()` must interrupt a blocked `epoll_wait()`. The `epoll_impl_t`
defined above already carries `wake_fd`; register it with `data.ptr == NULL` as
a sentinel:

```c
im->wake_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);

struct epoll_event ev = {
    .events = EPOLLIN | EPOLLET,
    .data.ptr = NULL,
};
epoll_ctl(im->epfd, EPOLL_CTL_ADD, im->wake_fd, &ev);
```

`wake()` writes one to the eventfd. `wait()` drains it when it sees the
sentinel.

`ready_enqueue_loop()` only links the task. `task_ready()`, `task_write()`,
`io_timer_add()`, and `io_loop_add_task()` are worker-thread operations, so the
loop is already running or about to run; an eventfd wake per enqueue would be a
wasted syscall. `io_loop_request_stop()` and `io_loop_exit()` still wake the
backend because they are cross-thread.

Adding now schedules the task's first drive. `io_loop_add_task()` enqueues the
task without changing its state, so `drive_task()` can run it at the next safe
point:

- `TASK_ACTIVE` performs its first read step;
- `TASK_READY` or `TASK_PROCESSING` runs the handler;
- a task attached before `io_loop_run()` is ready when the loop starts.

Do not call `drive_task()` inside `io_loop_add_task()`. The handler could complete
and free the task before `attach` returns, and attaching from inside a handler
would cause nested execution. Enqueueing keeps the call non-reentrant.

This is also why `drive_task()` is private. Attach registers and schedules;
`io_loop_run()` is the kickoff; `task_ready()` reschedules a live task. The loop
calls `drive_task()` internally.

## Teardown

The reaper still owns the lifetime decision. It runs only after the event batch,
and it skips tasks whose `removed` flag is set. Detach does not change that: an
event batch already holds `data.ptr` values, and `EPOLL_CTL_DEL` or `close()`
does not invalidate those pointers. Only deferring `task_free()` until the
batch is finished does.

The final split is: `task_finish()` marks the entry removed, detaches it from
the backend, and runs the callback. `loop_reap()` unlinks the removed entry and
`task_free()` closes the descriptor and frees the entry after the batch. This
keeps `data.ptr` valid for the whole batch:

```c
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

static void loop_reap(io_loop_t *loop) {
  io_task_entry_t *entry = loop->removed_head;
  loop->removed_head = NULL;
  while (entry != NULL) {
    io_task_entry_t *next = entry->removed_next;
    task_free(&entry->task);
    entry = next;
  }
}
```

For epoll, detach issues `EPOLL_CTL_DEL`; closing the fd would also remove it.
The explicit call is part of the backend contract, not the UAF fix. Buffers
remain the caller's.

## Phase 5 pain log

1. **The interface is readiness-shaped.** `read_step()` and `write_step()` run
   synchronously.
2. **No pending operation state.** A submitted recv/send needs to suspend and
   resume.
3. **`flush()` is a no-op for readiness.** io_uring will need it to submit
   queued operations.
4. **`park()` is generic.** It does not say what the task is waiting for, such
   as `POLLOUT`.
5. **Cancellation cannot abort an in-flight operation.** Closing an fd while
   the kernel still owns it is unsafe.
6. **The loop still cannot stop.**
7. **No timer.**
8. **TLS publication is implicit.**
9. **The public task struct contains engine fields.**
10. **Read and write are separate, but backend capability is not described.**

io_uring will break the readiness assumptions.

---

# Phase 6 — Add io_uring Without Pretending It Is epoll

The first io_uring attempt looks like epoll: submit a read and return `DONE`.
That is wrong. `io_uring_prep_recv()` only queues an operation. The bytes do not
exist until the completion is reaped. Add one step result:

```c
enum {
  IO_STEP_DONE,
  IO_STEP_AGAIN,
  IO_STEP_EOF,
  IO_STEP_ERROR,
  IO_STEP_PENDING, /* submitted; a completion will re-drive the task */
};
```

## Per-direction completion bookkeeping

A task can have one read and one write in flight at the same time. One shared
`op_*` triple is not enough. Use separate fields:

```c
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
int cancel_pending;
int io_error;
int inflight_ops; /* every op/watch that still owes a wake */
int batch_queued; /* already present in the current wait() output */

int removed;
int attached;
struct io_loop *owner;
struct io_task_entry *tasks_prev;
struct io_task_entry *next;
struct io_task_entry *removed_next;
```

These fields live in `io_task_entry_t`; `task_create()` zeroes them.
`inflight_ops` counts read, write, listener poll, and parked-write poll watches.
It is the cancellation and lifetime guard.

## Submit a read instead of performing one

```c
static int uring_read_step(io_loop_t *loop, io_task_t *task) {
  if (task->read_closed || task_entry(task)->read_inflight ||
      task->read_buffer == NULL || task->read_size == 0) {
    return IO_STEP_ERROR;
  }
  if (task->bytes_buffered == 0) {
    task->read_head = 0;
  }
  size_t len = task_read_free_span(task);
  if (len == 0) {
    return IO_STEP_ERROR;
  }

  struct io_uring_sqe *sqe = io_uring_get_sqe(&im->ring);
  if (sqe == NULL) {
    return IO_STEP_ERROR;
  }

  size_t tail = task_read_tail(task);
  io_uring_prep_recv(sqe, task->fd, task->read_buffer + tail, len, 0);
  sqe->user_data = (uint64_t)(uintptr_t)task | UD_READ;
  task_entry(task)->read_inflight = 1;
  task_entry(task)->inflight_ops++;

  return IO_STEP_PENDING;
}
```

The write side is the same with `io_uring_prep_send()`, `write_buffer +
write_off`, `write_len - write_off`, `UD_WRITE`, `write_inflight`.

## Consume completions in the state machine

The engine checks completions before it submits new work:

```c
case TASK_ACTIVE: {
  if (task_entry(task)->read_op_ready) {
    int st = task_entry(task)->read_op_result;
    task_entry(task)->read_op_ready = 0;

    if (st == IO_STEP_EOF) {
      task->read_closed = 1;
      task_entry(task)->read_eof_pending = 1;
    } else if (st == IO_STEP_ERROR) {
      task->state = TASK_COMPLETED;
      continue;
    }
  }

  if (task->bytes_buffered > 0 && !task_entry(task)->read_paused) {
    task->state = TASK_PROCESSING;
    continue;
  }
  /* ... then apply completion policy, submit reads, consume write completion ... */
}
```

A successful proactor read maps to `IO_STEP_AGAIN`, not `IO_STEP_DONE`:

```c
} else if (res > 0) {
  task->bytes_buffered += (size_t)res;
  task_entry(task)->read_op_result = IO_STEP_AGAIN;
  task_entry(task)->read_op_ready = 1;
}
```

The kernel completed one recv, not “all data currently available.” The handler
processes the read data; the engine submits a fresh read afterwards. That is why the
state machine must process buffered input before it submits another read.

## Reap operations in `wait()`

The low bits of `user_data` hold the operation kind:

```c
#define UD_KIND_MASK 7ULL

enum {
  UD_READ = 0,
  UD_WRITE = 1,
  UD_POLLIN = 2,
  UD_POLLOUT = 3,
  UD_WAKE = 4,
  UD_CANCEL = 5,
};
```

For a read completion, decrement `read_inflight` and `inflight_ops`, then store
the result:

```c
case UD_READ:
  if (task_entry(task)->read_inflight) {
    task_entry(task)->read_inflight = 0;
    task_entry(task)->inflight_ops--;
  }

  if (res == -EAGAIN || res == -EINTR) {
    if (uring_submit_read(loop, task) == 0) {
      continue;
    }
    task_entry(task)->read_op_result = IO_STEP_ERROR;
  } else if (res > 0) {
    task->bytes_buffered += (size_t)res;
    task_entry(task)->read_op_result = IO_STEP_AGAIN;
  } else if (res == 0) {
    task_entry(task)->read_op_result = IO_STEP_EOF;
  } else {
    task_entry(task)->read_op_result = IO_STEP_ERROR;
  }

  task_entry(task)->read_op_ready = 1;
  out[m++] = task;
  break;
```

A write `EAGAIN` is **not** resubmitted. Resubmitting a blocked send creates a
busy completion loop. Record `IO_STEP_AGAIN`, let the state machine set
`write_blocked`, and arm `POLLOUT`:

```c
case UD_WRITE:
  if (task_entry(task)->write_inflight) {
    task_entry(task)->write_inflight = 0;
    task_entry(task)->inflight_ops--;
  }

  if (res == -EAGAIN || res == -EINTR) {
    task_entry(task)->write_op_result = IO_STEP_AGAIN;
  } else if (res >= 0) {
    task->write_off += (size_t)res;
    task_entry(task)->write_op_result = IO_STEP_DONE;
  } else {
    task_entry(task)->write_op_result = IO_STEP_ERROR;
  }

  task_entry(task)->write_op_ready = 1;
  out[m++] = task;
  break;
```

A single task can have both completions in one batch. Coalesce it once:

```c
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
```

The engine sees both `read_op_ready` and `write_op_ready` in one drive pass,
which is the point of duplex I/O.

## Readiness-only tasks

A listener has no read buffer. It cannot submit a recv. Use a multishot `POLLIN`
watch in `attach()`:

```c
if (task->read_buffer == NULL) {
  struct io_uring_sqe *sqe = io_uring_get_sqe(&im->ring);
  if (sqe == NULL) {
    return -1;
  }
  io_uring_prep_poll_multishot(sqe, task->fd, POLLIN);
  sqe->user_data = (uint64_t)(uintptr_t)task | UD_POLLIN;
  task_entry(task)->watch_armed = 1;
  task_entry(task)->inflight_ops++;
}
```

A multishot completion with `IORING_CQE_F_MORE` keeps the watch armed. The final
completion clears `watch_armed` and drops `inflight_ops`. A listener always
starts in `TASK_PROCESSING`; it is driven by readability, not by a read step.

## Parked writes and `TASK_WAIT_WRITE`

There are two different write paths:

1. **Engine writes.** `task_write()` starts a write request. On epoll, the registered
   `EPOLLOUT` drives `write_step()`. On io_uring, completion drives
   `write_step()`; `EAGAIN` arms a `POLLOUT` watch.
2. **Handler-owned output.** nghttp2 and similar libraries write through a send
   callback. There is no `write_buffer`, but the handler still needs to be
   called when the socket becomes writable.

The second path needs one more state:

```c
typedef enum {
  TASK_ACTIVE,
  TASK_READY,
  TASK_PROCESSING,
  TASK_WAIT_WRITE, /* handler-owned output waits for writability */
  TASK_COMPLETED
} task_state_t;
```

On epoll:

```c
case TASK_WAIT_WRITE:
  if ((task_entry(task)->ready_events & IO_WRITE) != 0) {
    task_entry(task)->ready_events &= ~IO_WRITE;
    task->state = TASK_READY;
    continue;
  }
  /* still read while output is parked */
  break;
```

On io_uring, `park()` arms a one-shot `POLLOUT`:

```c
static void uring_park(io_loop_t *loop, io_task_t *task) {
  if (task_entry(task)->watch_armed) {
    return;
  }

  int want_write = task->state == TASK_WAIT_WRITE ||
                   (task->state == TASK_ACTIVE && task_entry(task)->write_blocked);
  if (!want_write || task->fd < 0) {
    return;
  }

  struct io_uring_sqe *sqe = io_uring_get_sqe(&im->ring);
  if (sqe != NULL) {
    io_uring_prep_poll_add(sqe, task->fd, POLLOUT);
    sqe->user_data = (uint64_t)(uintptr_t)task | UD_POLLOUT;
    task_entry(task)->watch_armed = 1;
    task_entry(task)->inflight_ops++;
  }
}
```

When the `POLLOUT` completion arrives:

```c
case UD_POLLOUT:
  task_entry(task)->ready_events |= IO_WRITE;
  task_entry(task)->write_blocked = 0;
  if (task_entry(task)->watch_armed) {
    task_entry(task)->watch_armed = 0;
    task_entry(task)->inflight_ops--;
  }
  break;
```

`TASK_WAIT_WRITE` is only for handler-owned output. An engine-managed active
write is still `TASK_ACTIVE`; a writable event drives `write_step()`, not the
handler.

Reads stay enabled in `TASK_WAIT_WRITE`. A response being blocked must not stop
the application from consuming input or observing EOF.

## Cancellation must wait for completions

`task_cancel()` cannot free a task while the kernel still references it.

```c
static int task_maybe_finish(io_task_t *task, int cancelled) {
  if (task_entry(task)->inflight_ops > 0) {
    task_abort_io(task);
    return 0;
  }
  task_finish(task, cancelled);
  return 1;
}
```

`task_abort_io()` sets `io_abort` and calls the backend's `cancel()` once:

```c
static void task_abort_io(io_task_t *task) {
  io_loop_t *loop = tl_loop;

  if (task_entry(task)->io_abort) {
    return;
  }
  task_entry(task)->io_abort = 1;

  if (task_entry(task)->inflight_ops > 0 && loop != NULL &&
      loop->backend != NULL && loop->backend->cancel != NULL) {
    loop->backend->cancel(loop, task);
  }
}
```

Phase 4's cancellation helper predates `inflight_ops`. The final form refuses to
release while the kernel still owns an operation:

```c
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
    return;
  }
  if (entry->inflight_ops > 0) {
    task_abort_io(task);
    return;
  }
  task_finish(task, 1);
}

static void task_complete_write(io_task_t *task) {
  task->write_active = 0;
  task->write_done = 1;
  task_entry(task)->write_blocked = 0;
}
```

For io_uring:

- pending recv/send and a parked `POLLOUT` are woken with
  `shutdown(fd, SHUT_RDWR)`;
- a listener multishot poll is removed with `IORING_OP_POLL_REMOVE`;
- the task stays alive until the final completion is reaped;
- `wait()` must discard completion payloads when `io_abort` or
  `cancel_requested` is set, but still decrement `inflight_ops`.

Closing the fd while an operation is in flight is a use-after-free waiting to
happen. `task_maybe_finish()` reaches `task_finish()` only when
`inflight_ops == 0`; the reaper then calls `task_free()`, which closes the
descriptor and frees the entry after the batch.

Every Phase 4 scheduler call to `task_finish()` becomes a call to
`task_maybe_finish()`. The cancellation guard in `drive_task()` is the clearest
example:

```c
if (task_entry(task)->cancel_requested ||
    task->state == TASK_COMPLETED) {
  int cancelled = task_entry(task)->cancel_requested;

  tl_current = prev_current;
  task_maybe_finish(task, cancelled);
  return;
}
```

## Complete the io_uring backend

The earlier snippets showed the shape. Replace those sketches of
`uring_read_step`, `uring_out_add`, and `uring_park` with the definitions below;
the backend also needs SQE submission, ring setup, timeout-aware waiting, and
cancellation. Keep the operation kind in the low three bits of `user_data`; task
pointers are at least 8-byte aligned:

```c
typedef struct {
  struct io_uring ring;
  int wake_fd;
  io_task_entry_t *cancel_head;
  io_task_entry_t *cancel_tail;
} uring_impl_t;

#define UD_KIND_MASK 7ULL
enum {
  UD_READ = 0,
  UD_WRITE = 1,
  UD_POLLIN = 2,
  UD_POLLOUT = 3,
  UD_WAKE = 4,
  UD_CANCEL = 5,
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
            task_entry(task)->cancel_pending = 0;
            if (im->cancel_pending_count > 0) {
              im->cancel_pending_count--;
            }
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
    return IO_STEP_ERROR;
  }
  return IO_STEP_PENDING;
}

static int uring_write_step(io_loop_t *loop, io_task_t *task) {
  if (uring_submit_write(loop, task) != 0) {
    return IO_STEP_ERROR;
  }
  return IO_STEP_PENDING;
}

static void uring_park(io_loop_t *loop, io_task_t *task) {
  if (task_entry(task)->watch_armed) {
    return;
  }
  int want_write = task->state == TASK_WAIT_WRITE ||
                   (task->state == TASK_ACTIVE &&
                    task_entry(task)->write_blocked);
  if (!want_write || task->fd < 0) {
    return;
  }
  struct io_uring_sqe *sqe = uring_sqe(loop);
  if (sqe != NULL) {
    io_uring_prep_poll_add(sqe, task->fd, POLLOUT);
    sqe->user_data = (uint64_t)(uintptr_t)task | UD_POLLOUT;
    task_entry(task)->watch_armed = 1;
    task_entry(task)->inflight_ops++;
  }
}

static void uring_cancel(io_loop_t *loop, io_task_t *task) {
  uring_impl_t *im = loop->impl;
  if (task->fd < 0) {
    return;
  }
  if (task->read_buffer == NULL && task_entry(task)->watch_armed) {
    uint64_t target = (uint64_t)(uintptr_t)task | UD_POLLIN;
    struct io_uring_sqe *sqe = uring_sqe(loop);
    if (sqe == NULL) {
      io_uring_submit(&im->ring);
      sqe = uring_sqe(loop);
    }
    if (sqe != NULL) {
      io_uring_prep_poll_remove(sqe, target);
      sqe->user_data = UD_CANCEL;
      io_uring_submit(&im->ring);
      return;
    }
    close(task->fd);
    task->fd = -1;
    return;
  }
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
```

This is the same contract as epoll, but the kernel owns the data operation.
`read_step()` and `write_step()` return `PENDING`; the completion sets
`read_op_ready`/`write_op_ready`, and the next `drive_task()` consumes it.

## Final write API

Phase 3's `task_write()` is the final write path: it starts one borrowed
request, and `write_done` reports that the active buffer is reusable.

Both functions are safe from inside the handler: a current task is not
re-enqueued, and the state machine continues on the existing stack.

## Complete the scheduler

Phase 3's `drive_task()` was readiness-only. The final version consumes
proactor completions, keeps reads alive while handler-owned output is parked,
and calls `park()` whenever the task yields. Replace the earlier body with this
one:

```c
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

```

The ready queue uses the same release gate. It skips a tombstoned entry and
routes every finished task through `task_maybe_finish()` so in-flight backend
work is aborted first:

```c
static void drive_task(io_task_t *task) {
  io_loop_t *loop = tl_loop;
  io_task_t *prev_current = tl_current;

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

      // Consume a proactor read completion.
      if (task_entry(task)->read_op_ready) {
        int st = task_entry(task)->read_op_result;
        task_entry(task)->read_op_ready = 0;
        if (st == IO_STEP_ERROR) {
          task->state = TASK_COMPLETED;
          continue;
        }
        if (st == IO_STEP_EOF) {
          task_mark_read_closed(task);
        }
      }

      // Hand buffered input, EOF, or a completed write to the handler before
      // considering automatic completion.
      if (task->bytes_buffered > 0 && !task_entry(task)->read_paused) {
        task->state = TASK_PROCESSING;
        continue;
      }
      if (task_entry(task)->read_eof_pending) {
        task->state = TASK_PROCESSING;
        continue;
      }
      if (task->write_done) {
        task->state = TASK_PROCESSING;
        continue;
      }

      // Submit/drain reads. Never submit past EOF, beyond capacity, or while a
      // proactor read is already outstanding.
      if (!task->read_closed && !task_entry(task)->read_stopped &&
          !task_entry(task)->read_inflight && task->read_buffer != NULL &&
          task->read_size > 0 && task->bytes_buffered < task->read_size) {
        int st = loop->backend->read_step(loop, task);

        if (st == IO_STEP_DONE) {
          task_entry(task)->read_paused = 0; // new input may unblock the handler
          continue; // keep draining until EAGAIN or the buffer is full
        }
        if (st == IO_STEP_ERROR) {
          task->state = TASK_COMPLETED;
          continue;
        }
        if (st == IO_STEP_EOF) {
          task_mark_read_closed(task);
          task->state = TASK_PROCESSING;
          continue;
        }
        if (st == IO_STEP_AGAIN && task->bytes_buffered > 0 &&
            !task_entry(task)->read_paused) {
          task->state = TASK_PROCESSING;
          continue;
        }
        // PENDING or EAGAIN with no data: continue to the write side.
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
      if (task->bytes_buffered == 0) {
        task_entry(task)->read_paused = 0;
      }

      if (task_entry(task)->read_op_ready) {
        int st = task_entry(task)->read_op_result;
        task_entry(task)->read_op_ready = 0;
        if (st == IO_STEP_ERROR) {
          task->state = TASK_COMPLETED;
          continue;
        }
        if (st == IO_STEP_EOF) {
          task_mark_read_closed(task);
        }
      }
      if (task->bytes_buffered > 0 && !task_entry(task)->read_paused) {
        task->state = TASK_PROCESSING;
        continue;
      }
      if (task_entry(task)->read_eof_pending) {
        task->state = TASK_PROCESSING;
        continue;
      }

      // Writable readiness here always belongs to handler-owned output (for
      // example nghttp2's send callbacks), so it must run the handler.
      if ((task_entry(task)->ready_events & READY_WRITE) != 0) {
        task_entry(task)->ready_events &= ~READY_WRITE;
        task->state = TASK_READY;
        continue;
      }

      // Keep reading while output is parked; a readable event is still
      // application data.
      if (!task->read_closed && !task_entry(task)->read_stopped &&
          !task_entry(task)->read_inflight && task->read_buffer != NULL &&
          task->read_size > 0 && task->bytes_buffered < task->read_size) {
        int st = loop->backend->read_step(loop, task);
        if (st == IO_STEP_DONE) {
          task_entry(task)->read_paused = 0;
          continue;
        }
        if (st == IO_STEP_ERROR) {
          task->state = TASK_COMPLETED;
          continue;
        }
        if (st == IO_STEP_EOF) {
          task_mark_read_closed(task);
          task->state = TASK_PROCESSING;
          continue;
        }
        if (st == IO_STEP_AGAIN && task->bytes_buffered > 0 &&
            !task_entry(task)->read_paused) {
          task->state = TASK_PROCESSING;
          continue;
        }
      }
      goto yield; // wait for writable or readable readiness
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
  if (loop != NULL && loop->backend != NULL && loop->backend->park != NULL) {
    loop->backend->park(loop, task);
  }
  tl_current = prev_current;
}
```

## Backend selection

```c
static void drive_task(io_task_t *task) {
  io_loop_t *loop = tl_loop;
  io_task_t *prev_current = tl_current;

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

      // Consume a proactor read completion.
      if (task_entry(task)->read_op_ready) {
        int st = task_entry(task)->read_op_result;
        task_entry(task)->read_op_ready = 0;
        if (st == IO_STEP_ERROR) {
          task->state = TASK_COMPLETED;
          continue;
        }
        if (st == IO_STEP_EOF) {
          task_mark_read_closed(task);
        }
      }

      // Hand buffered input, EOF, or a completed write to the handler before
      // considering automatic completion.
      if (task->bytes_buffered > 0 && !task_entry(task)->read_paused) {
        task->state = TASK_PROCESSING;
        continue;
      }
      if (task_entry(task)->read_eof_pending) {
        task->state = TASK_PROCESSING;
        continue;
      }
      if (task->write_done) {
        task->state = TASK_PROCESSING;
        continue;
      }

      // Submit/drain reads. Never submit past EOF, beyond capacity, or while a
      // proactor read is already outstanding.
      if (!task->read_closed && !task_entry(task)->read_stopped &&
          !task_entry(task)->read_inflight && task->read_buffer != NULL &&
          task->read_size > 0 && task->bytes_buffered < task->read_size) {
        int st = loop->backend->read_step(loop, task);

        if (st == IO_STEP_DONE) {
          task_entry(task)->read_paused = 0; // new input may unblock the handler
          continue; // keep draining until EAGAIN or the buffer is full
        }
        if (st == IO_STEP_ERROR) {
          task->state = TASK_COMPLETED;
          continue;
        }
        if (st == IO_STEP_EOF) {
          task_mark_read_closed(task);
          task->state = TASK_PROCESSING;
          continue;
        }
        if (st == IO_STEP_AGAIN && task->bytes_buffered > 0 &&
            !task_entry(task)->read_paused) {
          task->state = TASK_PROCESSING;
          continue;
        }
        // PENDING or EAGAIN with no data: continue to the write side.
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
      if (task->bytes_buffered == 0) {
        task_entry(task)->read_paused = 0;
      }

      if (task_entry(task)->read_op_ready) {
        int st = task_entry(task)->read_op_result;
        task_entry(task)->read_op_ready = 0;
        if (st == IO_STEP_ERROR) {
          task->state = TASK_COMPLETED;
          continue;
        }
        if (st == IO_STEP_EOF) {
          task_mark_read_closed(task);
        }
      }
      if (task->bytes_buffered > 0 && !task_entry(task)->read_paused) {
        task->state = TASK_PROCESSING;
        continue;
      }
      if (task_entry(task)->read_eof_pending) {
        task->state = TASK_PROCESSING;
        continue;
      }

      // Writable readiness here always belongs to handler-owned output (for
      // example nghttp2's send callbacks), so it must run the handler.
      if ((task_entry(task)->ready_events & READY_WRITE) != 0) {
        task_entry(task)->ready_events &= ~READY_WRITE;
        task->state = TASK_READY;
        continue;
      }

      // Keep reading while output is parked; a readable event is still
      // application data.
      if (!task->read_closed && !task_entry(task)->read_stopped &&
          !task_entry(task)->read_inflight && task->read_buffer != NULL &&
          task->read_size > 0 && task->bytes_buffered < task->read_size) {
        int st = loop->backend->read_step(loop, task);
        if (st == IO_STEP_DONE) {
          task_entry(task)->read_paused = 0;
          continue;
        }
        if (st == IO_STEP_ERROR) {
          task->state = TASK_COMPLETED;
          continue;
        }
        if (st == IO_STEP_EOF) {
          task_mark_read_closed(task);
          task->state = TASK_PROCESSING;
          continue;
        }
        if (st == IO_STEP_AGAIN && task->bytes_buffered > 0 &&
            !task_entry(task)->read_paused) {
          task->state = TASK_PROCESSING;
          continue;
        }
      }
      goto yield; // wait for writable or readable readiness
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
  if (loop != NULL && loop->backend != NULL && loop->backend->park != NULL) {
    loop->backend->park(loop, task);
  }
  tl_current = prev_current;
}
```

Replace Phase 1's `io_loop_new()` and `io_loop_free()` with these. The
constructor now takes a backend kind and tries each candidate in order;
`io_loop_free()` becomes private behind the shutdown API. `AUTO` prefers
io_uring and falls back to epoll:

```c
io_loop_t *io_loop_new(io_backend_kind_t kind) {
  io_loop_t *loop = calloc(1, sizeof *loop);
  if (loop == NULL) {
    return NULL;
  }

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
  if (loop->backend != NULL && loop->backend->fini != NULL) {
    loop->backend->fini(loop);
  }
  if (tl_loop == loop) {
    tl_loop = NULL;
  }
  free(loop);
}

const char *io_loop_backend_name(const io_loop_t *loop) {
  if (loop == NULL || loop->backend == NULL) {
    return "none";
  }
  return loop->backend->name;
}
```

`AUTO` prefers io_uring and falls back to epoll. `io_loop_backend_name()`
reports the winner. Both backends must pass the same duplex tests:
simultaneous read/write, `EAGAIN`, EOF-then-write, staged `write_done`, and
cancellation with both operations outstanding.

## Error status and deferred cancellation

Two io_uring/error edges are handled explicitly:

- `IO_STEP_ERROR` stores the failing syscall's negative errno in
  `entry->io_error`. `task_finish()` passes it to `on_complete()`; cancellation
  still uses `ECANCELED`, and successful completion uses `0`.
- If a listener's `POLL_REMOVE` cannot get an SQE, the backend sets
  `cancel_pending` instead of closing the fd. `uring_wait()` retries the removal
  when SQ space is available, keeping `watch_armed` and `inflight_ops` set until
  the final poll completion is reaped.

This keeps fd ownership with the task and gives callers a way to distinguish
normal completion, cancellation, and fatal I/O failure.

## Phase 6 pain log

1. **No timer.** Both backends can block forever.
2. **No stop/exit request.**
3. **No graceful drain.**
4. **No worker lifecycle.**
5. **Backend bookkeeping is public.**
6. **Readiness and completion interpret `AGAIN` differently.**
7. **No separate error status.** Fatal I/O completes the task.
8. **io_uring cancellation can run out of SQEs.** The backend defers
   `POLL_REMOVE` and retries when SQ space is available; it does not close the fd.
9. **The test matrix is mandatory.** An epoll-only test proves nothing about
   `POLLOUT`, cancellation, or multishot accounting.

---

# Phase 7 — Time, Wake, and Worker Lifetime

The engine is backend-neutral, but it needs a way to sleep until the next
deadline or wake event. A server needs three more things:

- one-shot timers for idle timeouts and drain deadlines;
- a cross-thread request to begin shutdown;
- a way to make the loop return after the current iteration.

## Time without a periodic tick

Per-connection deadlines belong in a min-heap. The loop sleeps until the
earliest timer deadline, and applications re-arm a timer when they need
periodic behavior.

```c
struct io_loop {
  /* ... */
  _Atomic int stop_requested;
  _Atomic int exit_requested;

  io_timer_t **timers;
  size_t timer_count;
  size_t timer_cap;
};
```

```c
static uint64_t engine_now_ms(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000u +
         (uint64_t)(ts.tv_nsec / 1000000L);
}

void io_loop_set_stop_handler(io_loop_t *loop, void (*cb)(io_loop_t *)) {
  if (loop == NULL) {
    return;
  }
  loop->stop_cb = cb;
}
```

`engine_now_ms()` uses `CLOCK_MONOTONIC`. Wall-clock time can jump backward and
is wrong for timeouts.

## The timer heap

Each timer is a heap node with an absolute deadline and an index back into the
heap, so cancellation is O(log N):

```c
typedef struct io_timer {
  struct io_loop *loop;
  uint64_t deadline_ms;
  void (*cb)(void *ctx);
  void *ctx;
  size_t index;
} io_timer_t;
```

```c
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
  while (loop->timer_count > 0 &&
         loop->timers[0]->deadline_ms <= now) {
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

io_timer_t *io_timer_add(io_loop_t *loop, uint64_t delay_ms,
                         void (*cb)(void *), void *ctx) {
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

```

`io_timer_add()` and `io_timer_cancel()` are loop-thread operations. Callbacks
run on the loop thread. A fired or cancelled timer handle must not be reused.
The loop frees pending timers during shutdown without running them.

The server uses one idle timer per connection. The callback compares
`last_app_activity_ms` with the idle limit and either performs the idle action
or re-arms for the remaining time. Keepalive and drain housekeeping use an
on-demand timer only while those features are active, rather than an always-on
tick.

## Make the wait timeout-aware

The loop computes how long the backend may block:

```c
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
```

The 60-second cap keeps a very distant timer from turning into a wait that
ordinary scheduling mistakes cannot interrupt. The actual deadline is checked
on every iteration.

## Stop is a request, exit is a decision

Separate the two concepts:

- `io_loop_request_stop()` asks the owner to begin shutdown;
- `io_loop_exit()` makes `io_loop_run()` return.

```c
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
```

With a stop handler, the handler owns the drain and decides when to call
`io_loop_exit()`. Without one, stop means exit immediately.

The backend wake is essential. A signal thread cannot set a flag and hope a
worker blocked in `epoll_wait()` or `io_uring_wait_cqe()` notices.

## Forced shutdown

Graceful drain policy belongs to the application stop handler and timers. The
engine also provides a forced shutdown for the loop thread: cancel every task, run callbacks, drain
in-flight completions, and reap everything.

```c
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


```

`io_loop_shutdown()` must not be called from inside `io_loop_run()` or a handler.
It is meant for the worker thread after the run loop has returned, or before the
loop starts.

## Worker lifetime

Create the loop inside the worker thread that owns it:

```c
static void *worker_thread_main(void *arg) {
  io_loop_t *loop = io_loop_new(IO_BACKEND_AUTO);
  if (loop == NULL) {
    return NULL;
  }

  io_loop_set_stop_handler(loop, server_stop);
  server_register_loop(loop);
  setup_accept_task_async(loop, listen_fd);
  server_house_arm(loop);

  io_loop_run(loop);

  server_unregister_loop(loop);
  io_loop_shutdown(loop);
  return NULL;
}
```

The order matters:

- set the stop handler before publishing the loop, because another thread may
  request stop immediately;
- register the loop before running it, so an early signal is visible;
- unregister after `io_loop_run()` and before `io_loop_shutdown()`, so
  the registry never holds a freed pointer.

`io_loop_shutdown()` cancels/reaps anything left after the run loop
returns, then releases the loop.

## Phase 7 pain log

1. **Timers are one-shot.** Periodic behavior must re-arm explicitly.
2. **Forced shutdown must cancel/reap tasks before release.**
3. **Keepalive and drain policy still belong to the application.**
4. **Backend errors are not surfaced uniformly.**
5. **Timer operations are loop-thread only.**
6. **The loop is single-threaded per worker by design.**
7. **Task cancellation during shutdown needs both backends tested.**
8. **The public task struct is still easy to confuse with its private entry.**
9. **No built-in connection abstraction.**
10. **The header is now the real contract.**

---

# Phase 8 — Close the Library

At this point the header is the product. It is not a summary of the `.c` file;
it is what a caller can rely on.

## The public header

```c
#ifndef ASYNC_ENGINE_H
#define ASYNC_ENGINE_H

#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>

#define MAX_EVENTS 1024

typedef enum {
  TASK_ACTIVE,     /* engine-managed reads and/or writes may progress */
  TASK_READY,      /* explicitly scheduled: run the handler next */
  TASK_PROCESSING, /* an event/post-step is ready for the handler */
  TASK_WAIT_WRITE, /* handler-owned output waits for writability */
  TASK_COMPLETED   /* finish and release */
} task_state_t;

struct io_task;

typedef struct io_task {
  int fd;
  task_state_t state;

  uint8_t *read_buffer;  /* caller-owned ring */
  size_t read_size;
  size_t read_head;
  size_t bytes_buffered;

  uint8_t *write_buffer; /* borrowed active write */
  size_t write_size;
  size_t write_len;
  size_t write_off;
  int write_active;

  int write_done;
  int read_closed;

  void *context;

  void (*process_handler)(struct io_task *task);
  void (*on_complete)(void *context, int status, uint8_t *read_buffer);
  void (*on_cancel)(void *context);

  /* Scheduler and backend bookkeeping lives in the private task entry. */
} io_task_t;

typedef struct io_loop io_loop_t;
typedef struct io_timer io_timer_t;

typedef enum {
  IO_BACKEND_AUTO = 0,
  IO_BACKEND_EPOLL,
  IO_BACKEND_URING,
} io_backend_kind_t;

io_task_t *task_create(void);
void task_free(io_task_t *task);

void task_ready(io_task_t *task);
int task_start_read(io_task_t *task);
void task_stop_read(io_task_t *task);

static inline size_t task_read_span(const io_task_t *task) {
  if (task == NULL || task->read_size == 0) {
    return 0;
  }
  size_t span = task->read_size - task->read_head;
  return span < task->bytes_buffered ? span : task->bytes_buffered;
}
int task_read_consume(io_task_t *task, size_t n);
int task_setup_read(io_task_t *task, uint8_t *read_buffer, size_t read_size);

int task_write(io_task_t *task, const uint8_t *buf, size_t len);
void task_cancel(io_task_t *task);
int is_task_cancel_requested(const io_task_t *task);

io_loop_t *io_loop_new(io_backend_kind_t kind);

io_loop_t *io_loop_current(void);
const char *io_loop_backend_name(const io_loop_t *loop);

int io_loop_add_task(io_loop_t *loop, io_task_t *task);

void io_loop_run(io_loop_t *loop);
void io_loop_shutdown(io_loop_t *loop);
io_timer_t *io_timer_add(io_loop_t *loop, uint64_t delay_ms,
                         void (*cb)(void *), void *ctx);
void io_timer_cancel(io_timer_t *timer);
void io_loop_set_stop_handler(io_loop_t *loop, void (*cb)(io_loop_t *));
void io_loop_request_stop(io_loop_t *loop);
int io_loop_stop_requested(const io_loop_t *loop);
void io_loop_exit(io_loop_t *loop);

#endif
```

## How the header is organized

The API falls into seven groups:

1. **Task construction and state** — `task_create`, `task_free`,
   `io_task_t`, `task_state_t`.
2. **Scheduling** — `task_ready`; `io_loop_add_task()` schedules the first run.
3. **Reads** — `task_start_read`, `task_stop_read`, `task_read_span`,
   `task_read_consume`, `read_closed`.
4. **Writes** — `task_write`, `write_done`.
5. **Cancellation** — `task_cancel`, `is_task_cancel_requested`, `on_cancel`.
6. **Loop lifecycle** — `io_loop_new`, `io_loop_shutdown`,
   `io_loop_current`, `io_loop_backend_name`, `io_loop_run`.
7. **I/O registration, timers, and shutdown** — `io_loop_add_task`,
   `io_timer_add`, `io_timer_cancel`, `io_loop_set_stop_handler`,
   `io_loop_request_stop`,
   `io_loop_stop_requested`, `io_loop_exit`.

The header says nothing about epoll, io_uring, eventfds, or readiness
structures.

## Ownership and lifetime

- The caller obtains `io_task_t` from `task_create()` once.
- The caller sets `fd`, `state`, `context`, handlers, and callbacks.
- Read storage is installed later with `task_setup_read()`; it is not required at
  add time.
- The caller owns `read_buffer` and `write_buffer`. The engine never frees them.
- On successful `io_loop_add_task()`, the engine owns the task and closes its fd;
  add binds and schedules the task, but registers no interest.
- `task_start_read()` starts or restarts read interest; `task_stop_read()`
  temporarily disarms it.
- `task_write()` borrows the active buffer until `write_done`.
- `task_ready()`, `task_start_read()`, `task_stop_read()`, `task_write()` and
  `task_cancel()` are worker-thread operations.
- `io_loop_request_stop()` and `io_loop_exit()` are safe from another thread;
  they use atomics and the backend wake path.
- `on_cancel()` runs when cancellation was requested and it exists; otherwise
  `on_complete()` receives `ECANCELED`.
- Fatal I/O failures call `on_complete()` with a negative errno status; successful
  completion uses `0`.
- The engine frees the task entry after the callback. The callback frees the
  buffers.
- `task_free()` is only for a failed `io_loop_add_task()`. It closes the fd and
  releases the entry; do not close the fd yourself. After a successful add, the
  engine releases both.
- `io_loop_shutdown()` cancels/reaps tasks and releases the loop; call
  it from the loop thread.

## State machine contract

A task is always in one of five states:

- `TASK_ACTIVE`: the engine may submit reads and drive an engine-managed write.
  Buffered input or `write_done` takes priority and runs the handler.
- `TASK_READY`: explicitly scheduled; run the handler next.
- `TASK_PROCESSING`: run the handler after a read or write step.
- `TASK_WAIT_WRITE`: handler-owned output is blocked. A writable event runs the
  handler. Reads remain active where possible.
- `TASK_COMPLETED`: release after callbacks and in-flight operations finish.

The handler contract:

- consume from `read_head`, using `task_read_span()` for the wrap boundary, then
  call `task_read_consume(task, n)` for the processed bytes;
- leave residual bytes in place; reads continue into free ring space, and
  `task_ready()` requests a handler pass if there is no new edge;
- call `task_write()` to start one write request;
- clear `write_done`, start the next write request, or finish the task;
- set `TASK_COMPLETED` when the task is done;
- return to `TASK_ACTIVE` to let the engine continue;
- use `TASK_WAIT_WRITE` when the handler itself owns the output path;
- never block: the handler runs on the worker's only loop thread.

Two conditions deserve emphasis:

- A read EOF is a half-close. The write side can continue, and the handler
  decides when the task is complete.

## A library user

The following is application code, not part of `async_engine.c`:

```c
#include "async_engine.h"
#include <string.h>

static const uint8_t reply[] = "hello";

static void on_timeout(void *ctx) {
  /* idle timeout, keepalive, drain deadline */
}

static void on_complete(void *context, int status, uint8_t *read_buffer) {
  (void)status;
  free(context);
  free(read_buffer);
}

static void on_process(io_task_t *task) {
  if (task->bytes_buffered > 0) {
    size_t n = task->bytes_buffered;
    size_t span = task_read_span(task);
    uint8_t *p = task->read_buffer + task->read_head;
    /* Parse p[0..span); if span < n, continue at task->read_buffer[0]. */
    task_read_consume(task, n);
  }

  if (task->write_done) {
    task->write_done = 0;
  }

  if (!task->write_active) {
    task_write(task, reply, sizeof(reply) - 1);
  }

  if (task->read_closed && !task->write_active &&
      task->bytes_buffered == 0) {
    task->state = TASK_COMPLETED;
    return;
  }
  task->state = TASK_ACTIVE;
}

static int run_worker(int fd) {
  io_loop_t *loop = io_loop_new(IO_BACKEND_AUTO);
  if (loop == NULL) {
    close(fd);
    return -1;
  }

  io_task_t *task = task_create();
  if (task == NULL) {
    close(fd);
    io_loop_shutdown(loop);
    return -1;
  }

  task->fd = fd;
  task->state = TASK_ACTIVE;
  task->context = calloc(1, 1);
  uint8_t *read_buffer = malloc(16384);
  if (read_buffer == NULL || task->context == NULL) {
    free(read_buffer);
    free(task->context);
    task_free(task); // closes fd
    io_loop_shutdown(loop);
    return -1;
  }

  task->process_handler = on_process;
  task->on_complete = on_complete;

  if (task_setup_read(task, read_buffer, 16384) != 0 ||
      io_loop_add_task(loop, task) != 0) {
    free(read_buffer);
    free(task->context);
    task_free(task); // closes fd
    io_loop_shutdown(loop);
    return -1;
  }
  if (task_start_read(task) != 0) {
    task_cancel(task); // on_complete frees the buffers/context
    io_loop_shutdown(loop);
    return -1;
  }

  if (io_timer_add(loop, 100, on_timeout, NULL) == NULL) {
    /* handle allocation failure */
  }
  io_loop_run(loop);
  io_loop_shutdown(loop);

  return 0;
}
```

The caller never includes `epoll.h` or `liburing.h`. It never sees the backend
vtable. It still has to understand the task state machine, because that is the
engine's public behavior.

## The write queue lives above the engine

The engine holds exactly one active write. The application decides whether to
queue, drop, coalesce, or backpressure.

- the active buffer must stay valid until `write_done`;
- caller queue slots may not move while active;
- a cross-thread producer must enqueue under a lock and wake the worker before
  calling `task_write()` on the worker thread;
- the worker retries the next request after `write_done`;
- completion is explicit: stop calling `task_write()` and finish the task.

## What the tutorial deliberately leaves imperfect

This is a teaching engine, not a finished general-purpose runtime:

- backend bookkeeping lives in the public task struct;
- one active write request is supported; queuing remains caller-side policy;
- timers are one-shot; there is no built-in repeating timer;
- the loop is worker-pinned, not a work-stealing scheduler;
- `io_loop_shutdown()` owns task cancellation and loop release;
- fatal I/O errors are reported as negative `on_complete` status;
- io_uring defers `POLL_REMOVE` when no SQE is available instead of closing the fd.

Those are reasonable boundaries for a server engine that already knows its
consumers.

## What you should be able to explain now

If you built the module phase by phase, you should be able to explain:

- why reads and writes need independent buffers and progress counters;
- why a task can be half-closed and still write;
- why `write_done` is per write request;
- why the engine does not own a write queue;
- why epoll readiness and io_uring completions need different step results;
- why a ready queue is separate from fd readiness;
- why `task_ready()` is worker-local while `io_loop_request_stop()` is not;
- why cancellation must wait for every in-flight proactor operation;
- why `TASK_WAIT_WRITE` runs the handler instead of the engine write step;
- why the public header contains no epoll or io_uring types;
- how to write a library user without putting the engine in `main()`.

That is the point of the tutorial: not to memorize the final file, but to be
able to rebuild it, explain its seams, and know which parts are policy and
which parts are mechanism.
