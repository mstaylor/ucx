#include "tcpunch.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdbool.h>

atomic_bool connection_established = ATOMIC_VAR_INIT(false);
atomic_int accepting_socket = ATOMIC_VAR_INIT(-1);

void* peer_listen(void* p) {
    PeerConnectionData* info = (PeerConnectionData*)p;

    // Create socket on the port that was previously used to contact the rendezvous server
    int listen_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_socket == -1) {
        ucs_error("Socket creation failed: ");
        return UCS_ERR_IO_ERROR;
    }
    int enable_flag = 1;
    if (setsockopt(listen_socket, SOL_SOCKET, SO_REUSEADDR, &enable_flag, sizeof(int)) < 0 ||
        setsockopt(listen_socket, SOL_SOCKET, SO_REUSEPORT, &enable_flag, sizeof(int)) < 0) {
        ucs_error("Setting REUSE options failed: ");
        return UCS_ERR_IO_ERROR;
    }

    struct sockaddr_in local_port_data{};
    local_port_data.sin_family = AF_INET;
    local_port_data.sin_addr.s_addr = INADDR_ANY;
    local_port_data.sin_port = info->port;

    if (bind(listen_socket, (const struct sockaddr *)&local_port_data, sizeof(local_port_data)) < 0) {
        ucs_error("Could not bind to local port: ");
        return UCS_ERR_IO_ERROR;
    }

    if (listen(listen_socket, 1) == -1) {
        ucs_error("Listening on local port failed: ");
        return UCS_ERR_IO_ERROR;
    }

    struct sockaddr_in peer_info{};
    unsigned int len = sizeof(peer_info);

    while(true) {
        int peer = accept(listen_socket, (struct sockaddr*)&peer_info, &len);
        if (peer == -1) {

            ucs_error("Error when connecting to peer %s", strerror(errno));

        } else {

            ucs_warn("Succesfully connected to peer, accepting");

            atomic_store(&accepting_socket, peer);
            atomic_store(&connection_established, true);
            return 0;
        }
    }
}

int pair(const char * pairing_name, const char * server_address, int port, int timeout_ms) {
    return 0;
}
