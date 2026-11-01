#ifndef SERVER_PRIVATE_H
#define SERVER_PRIVATE_H

/* The worker's async engine handle is now owned by the engine itself:
 * each thread calls io_loop_new() and reaches it via io_loop_current(). */

#endif
