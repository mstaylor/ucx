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

typedef struct {
    struct in_addr ip;
    in_port_t      port;
} PeerConnectionData;

int msleep(long msec);

char * ip_to_string(in_addr_t *ip, char * buffer, size_t max_size);

#endif //UCX_NAT_TRAVERSAL_H
