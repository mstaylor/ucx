//
// Created by qad5gv on 1/31/24.
//

#ifndef UCX_TCPUNCH_H
#define UCX_TCPUNCH_H

#include "tcp.h"
#include <netinet/in.h>
#include <sys/socket.h>

#include <arpa/inet.h>


typedef struct {
    struct in_addr ip;
    in_port_t      port;
} PeerConnectionData;



int pair(const char * pairing_name, const char* server_address, int port, int timeout_ms);

#endif //UCX_TCPUNCH_H
