#ifndef SERVER_H
#define SERVER_H

#include "custring.h"

/* Initialize and run the TCP server on the specified port.
 * Returns 0 after a clean shutdown, or -1 on configuration, listener,
 * worker-startup, or signal-wait failure. */
int Server_Init(FixedString5 *port);

#endif
