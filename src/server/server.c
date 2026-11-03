#include "server.h"
#include "async_engine.h"
#include "config.h"
#include "custring.h"
#include "liblog.h"
#include "client_session.c"
#include "create_listener_socket.c"
#include <netdb.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
  FixedString5 *port;
  int listen_fd;
} worker_arg_t;

static pthread_mutex_t g_start_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_start_cv = PTHREAD_COND_INITIALIZER;
static int g_start_done = 0;
static int g_start_failed = 0;

static void worker_startup_result(int ok) {
  pthread_mutex_lock(&g_start_mu);
  g_start_done++;
  if (!ok) {
    g_start_failed++;
  }
  pthread_cond_broadcast(&g_start_cv);
  pthread_mutex_unlock(&g_start_mu);
}

static int wait_for_workers(int expected) {
  pthread_mutex_lock(&g_start_mu);
  while (g_start_done < expected) {
    pthread_cond_wait(&g_start_cv, &g_start_mu);
  }
  int failed = g_start_failed;
  pthread_mutex_unlock(&g_start_mu);
  return failed;
}

// H2_IO_BACKEND=epoll|uring selects the worker's async I/O backend; unset means
// auto (io_uring with an epoll fallback).
static io_backend_kind_t choose_backend(void) {
  const char *e = getenv("H2_IO_BACKEND");
  if (e != NULL && strcmp(e, "epoll") == 0) {
    return IO_BACKEND_EPOLL;
  }
  if (e != NULL && strcmp(e, "uring") == 0) {
    return IO_BACKEND_URING;
  }
  return IO_BACKEND_AUTO;
}

void accept_connections_handler(io_task_t *task) {
  io_loop_t *loop = io_loop_current();
  while (true) {
    if (atomic_load_explicit(&g_shutdown, memory_order_relaxed)) {
      task->state = TASK_PROCESSING;
      return;
    }

    // Non-blocking accept call
    int client_fd = accept(task->fd, NULL, NULL);

    if (client_fd >= 0) {
      if (!client_conn_admit(client_fd)) {
        continue; // over the connection limit: fd already closed
      }
      if (setup_client_session_async(loop, client_fd) != 0) {
        client_conn_release();
      }
      continue; // Loop to check if more connections are queued!
    }

    if (client_fd == -1) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        // We drained the OS backlog! Reset state and yield to the event loop.
        task->state = TASK_PROCESSING;
        return;
      }
      if (errno == EINTR) {
        continue; // Interrupted by system signal, try accept() again
      }
      // Real error handling (e.g. EMFILE / out of file descriptors)
      perror("accept error");
      task->state = TASK_PROCESSING;
      return;
    }
  }
}

static void accept_task_complete(void *context, int status,
                                 uint8_t *read_buffer) {
  (void)context;
  (void)status;
  (void)read_buffer;
  tl_accept_task = NULL;
}

int setup_accept_task_async(io_loop_t *loop, int listen_fd) {
  int flags = fcntl(listen_fd, F_GETFL, 0);
  if (flags < 0 || fcntl(listen_fd, F_SETFL, flags | O_NONBLOCK) != 0) {
    log_error("[*] accept: failed to make listener non-blocking");
    close(listen_fd);
    return -1;
  }

  io_task_t *task = task_create();
  if (task == NULL) {
    log_error("[*] accept task allocation failed");
    close(listen_fd);
    return -1;
  }

  task->fd = listen_fd;
  task->state = TASK_PROCESSING;
  task->read_size = 0;
  task->read_buffer = NULL;
  task->bytes_buffered = 0;
  task->write_buffer = NULL;
  task->write_size = 0;
  task->write_len = 0;
  task->write_off = 0;

  task->context = NULL; // e.g., task->context = app_context;

  task->process_handler = accept_connections_handler;
  task->on_complete = accept_task_complete;

  if (io_loop_add_task(loop, task) != 0) {
    task_free(task); /* closes listen_fd */
    return -1;
  }
  if (task_start_read(task) != 0) {
    task->context = NULL;
    task->on_complete = NULL;
    task_cancel(task); /* closes listen_fd */
    return -1;
  }

  tl_accept_task = task;
  return 0;
}

void *worker_thread_main(void *arg) {
  worker_arg_t *wa = arg;
  int listen_fd = wa->listen_fd;

  // 1. Create a private async engine (and its backend) for THIS thread.
  io_loop_t *loop = io_loop_new(choose_backend());
  if (loop == NULL) {
    log_error("[*] worker: no async backend available");
    close(listen_fd);
    worker_startup_result(0);
    return (void *)(intptr_t)-1;
  }
  log_info("[*] worker async backend: %s", io_loop_backend_name(loop));

  // 2. Shutdown starts on the loop thread; housekeeping uses timers.
  io_loop_set_stop_handler(loop, server_stop);
  server_register_loop(loop);

  // 3. Register THIS thread's private listening socket with its private loop.
  if (setup_accept_task_async(loop, listen_fd) != 0) {
    log_error("[*] worker: failed to register accept task");
    server_unregister_loop(loop);
    io_loop_shutdown(loop);
    http2_thread_cleanup();
    worker_startup_result(0);
    return (void *)(intptr_t)-1;
  }
  server_house_arm(loop);

  // If shutdown was requested while this worker was starting, stop promptly.
  if (atomic_load_explicit(&g_shutdown, memory_order_relaxed)) {
    io_loop_request_stop(loop);
  }
  worker_startup_result(1);

  // 4. Run loop with ZERO thread lock contention!
  io_loop_run(loop);
  server_unregister_loop(loop);
  io_loop_shutdown(loop);
  http2_thread_cleanup();
  return NULL;
}

// Initialize Server
int Server_Init(FixedString5 *port) {
  pthread_mutex_lock(&g_start_mu);
  g_start_done = 0;
  g_start_failed = 0;
  pthread_mutex_unlock(&g_start_mu);

  server_limits_init();
  if (g_stream_window == 0 || g_connection_window == 0) {
    log_error("[*] H2_INITIAL_WINDOW_SIZE and H2_CONN_WINDOW_SIZE must be nonzero");
    return -1;
  }
  if (server_tls_init() < 0) {
    return -1;
  }

  int listen_fds[THREAD_COUNT];
  for (int i = 0; i < THREAD_COUNT; i++) {
    listen_fds[i] = -1;
  }
  for (int i = 0; i < THREAD_COUNT; i++) {
    listen_fds[i] = create_listener_socket(port);
    if (listen_fds[i] < 0) {
      log_error("[*] failed to create listener %d", i);
      for (int j = 0; j < i; j++) {
        close(listen_fds[j]);
      }
      server_tls_free();
      return -1;
    }
  }
  log_info("[*] Server listening on port %s", port->data);

  // Block SIGINT/SIGTERM in every thread (workers inherit the mask) and wait
  // for one here; sigwait() is the only handler, so no async-signal-safety
  // constraints leak into the shutdown path.
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGINT);
  sigaddset(&set, SIGTERM);
  pthread_sigmask(SIG_BLOCK, &set, NULL);

  pthread_t threads[THREAD_COUNT];
  worker_arg_t args[THREAD_COUNT];
  int started = 0;
  for (int i = 0; i < THREAD_COUNT; i++) {
    args[i].port = port;
    args[i].listen_fd = listen_fds[i];
    if (pthread_create(&threads[i], NULL, worker_thread_main, &args[i]) != 0) {
      log_error("[*] failed to create worker %d", i);
      for (int j = i; j < THREAD_COUNT; j++) {
        close(listen_fds[j]);
      }
      break;
    }
    started++;
  }

  int startup_failed = wait_for_workers(started);
  if (started != THREAD_COUNT || startup_failed != 0) {
    log_error("[*] worker startup failed (%d/%d started, %d failed)", started,
              THREAD_COUNT, startup_failed);
    server_request_shutdown();
    for (int i = 0; i < started; i++) {
      pthread_join(threads[i], NULL);
    }
    server_tls_free();
    return -1;
  }

  int sig = 0;
  int rc = sigwait(&set, &sig);
  if (rc != 0) {
    log_error("sigwait failed: %s", strerror(rc));
  } else {
    log_info("[*] signal %d: draining connections", sig);
  }
  server_request_shutdown();

  int worker_failed = 0;
  for (int i = 0; i < started; i++) {
    void *result = NULL;
    pthread_join(threads[i], &result);
    if (result != NULL) {
      worker_failed = 1;
    }
  }
  server_tls_free();
  return rc == 0 && !worker_failed ? 0 : -1;
}
