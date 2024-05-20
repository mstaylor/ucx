//
// Created by parallels on 3/28/24.
//

#ifndef UCX_NAT_TRAVERSAL_H
#define UCX_NAT_TRAVERSAL_H

#include "tcp.h"
#include <netinet/in.h>
#include <sys/socket.h>

#include <arpa/inet.h>
#include <stdatomic.h>
#include <stdbool.h>

#define NAT_RETRIES 6
#define NAT_CONNECT_TO_SEC 6

typedef struct {
    struct in_addr ip;
    in_port_t      port;
} PeerConnectionData;

typedef struct {
  struct in_addr ip;
  in_port_t      port;
  atomic_int accepting_socket;
  atomic_bool connection_established;
} PeerConnectionData2;

int msleep(long msec);

const char * ip_to_string(in_addr_t *ip, char * buffer, size_t max_size);


/**
 * Checks for peer address tries to connect to the remote peer
 * @param p
 */
void listen_for_updates_peer(void *p);




#endif //UCX_NAT_TRAVERSAL_H
