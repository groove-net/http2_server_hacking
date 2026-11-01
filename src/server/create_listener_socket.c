#include "../../include/custring.h"
#include <fcntl.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define BACKLOG SOMAXCONN // Use the kernel's maximum pending-connection queue.

int create_listener_socket(FixedString5 *port) {
  struct addrinfo hints;
  struct addrinfo *servinfo = NULL;

  memset(&hints, 0, sizeof hints);
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE;

  int rv = getaddrinfo(NULL, port->data, &hints, &servinfo);
  if (rv) {
    fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
    return -1;
  }

  int sockfd = -1;
  int yes = 1;
  struct addrinfo *p;
  // loop through all the results and bind to the first we can
  for (p = servinfo; p != NULL; p = p->ai_next) {
    sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
    if (sockfd == -1) {
      perror("server: socket");
      continue;
    }

    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1) {
      perror("setsockopt SO_REUSEADDR");
      close(sockfd);
      freeaddrinfo(servinfo);
      return -1;
    }

    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(int)) == -1) {
      perror("setsockopt SO_REUSEPORT");
      close(sockfd);
      freeaddrinfo(servinfo);
      return -1;
    }

    if (bind(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
      close(sockfd);
      sockfd = -1;
      perror("server: bind");
      continue;
    }

    break;
  }

  freeaddrinfo(servinfo); // all done with this structure

  if (p == NULL || sockfd < 0) {
    fprintf(stderr, "server: failed to bind\n");
    return -1;
  }

  if (listen(sockfd, BACKLOG) == -1) {
    perror("listen");
    close(sockfd);
    return -1;
  }

  return sockfd;
}
