#include "tcpunch.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>

atomic_bool connection_established = ATOMIC_VAR_INIT(false);
atomic_int accepting_socket = ATOMIC_VAR_INIT(-1);

void* peer_listen(void* p) {
    return NULL;
}

int pair(const char * pairing_name, const char * server_address, int port, int timeout_ms) {
    return 0;
}
