//
// Created by qad5gv on 1/31/24.
//

#ifndef UCX_TCPUNCH_H
#define UCX_TCPUNCH_H

#include "tcp.h"
#include <netinet/in.h>
#include <sys/socket.h>

#include <arpa/inet.h>
#include <stdatomic.h>


typedef struct {
    struct in_addr ip;
    in_port_t      port;
} PeerConnectionData;

atomic_bool connection_established;
atomic_bool end_connection;
atomic_int accepting_socket;

int connectandBindLocal(PeerConnectionData * data, struct sockaddr_storage *saddr, const char * pairing_name, const char* server_address, int port, int timeout_ms);

int pair(int *peer_socket, struct sockaddr_storage *saddr, const char * pairing_name, const char* server_address, int port, int timeout_ms);

char * ip_to_string(in_addr_t *ip, char * buffer, size_t max_size);

ucs_status_t peer_listen(void* p);



int msleep(long msec);

#endif //UCX_TCPUNCH_H
