//
// Created by qad5gv on 1/31/24.
//

#ifndef UCX_TCPUNCH_H
#define UCX_TCPUNCH_H


#include <netinet/in.h>
#include <sys/socket.h>

#include <arpa/inet.h>


#define DEBUG 0



int pair(const char * pairing_name, const char* server_address, int port = 10000, int timeout_ms = 0);

#endif //UCX_TCPUNCH_H
