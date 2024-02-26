#include "tcpunch.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <string.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdbool.h>
#include <time.h>
#include <errno.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>



connection_established = ATOMIC_VAR_INIT(false);
end_connection = ATOMIC_VAR_INIT(false);
accepting_socket = ATOMIC_VAR_INIT(-1);




ucs_status_t peer_listen(void* p) {
    struct sockaddr_in peer_info;
    struct sockaddr_in local_port_data;
    int peer;

    unsigned int len;
    int enable_flag = 1;
    PeerConnectionData* info = (PeerConnectionData*)p;

    // Create socket on the port that was previously used to contact the rendezvous server
    int listen_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_socket == -1) {
        ucs_error("Socket creation failed: ");
        return UCS_ERR_IO_ERROR;
    }

    if (setsockopt(listen_socket, SOL_SOCKET, SO_REUSEADDR, &enable_flag, sizeof(int)) < 0 ||
        setsockopt(listen_socket, SOL_SOCKET, SO_REUSEPORT, &enable_flag, sizeof(int)) < 0) {
        ucs_error("Setting REUSE options failed: ");
        return UCS_ERR_IO_ERROR;
    }


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


    len = sizeof(peer_info);

    while(true) {
        peer = accept(listen_socket, (struct sockaddr*)&peer_info, &len);
        if (peer == -1) {
            ucs_error("Error when connecting to peer %s", strerror(errno));
        } else {
            ucs_warn("Succesfully connected to peer, accepting");
            atomic_store(&accepting_socket, peer);
            atomic_store(&connection_established, true);
            return UCS_OK;
        }
    }


}

char * ip_to_string(in_addr_t *ip, char * buffer, size_t max_size) {
    inet_ntop(AF_INET, ip, buffer, max_size);
    return buffer;
}

/* msleep(): Sleep for the requested number of milliseconds. */
int msleep(long msec)
{
    struct timespec ts;
    int res;

    if (msec < 0)
    {
        errno = EINVAL;
        return -1;
    }

    ts.tv_sec = msec / 1000;
    ts.tv_nsec = (msec % 1000) * 1000000;

    do {
        res = nanosleep(&ts, &ts);
    } while (res && errno == EINTR);

    return res;
}

int connectandBindLocal(PeerConnectionData * data, struct sockaddr_storage *saddr, const char * pairing_name, const char* server_address, int port, int timeout_ms) {
    struct sockaddr_in *sa_in;
    struct timeval timeout;

    struct sockaddr_in server_data;
    PeerConnectionData public_info;
    ssize_t bytes;
    char ipadd[UCS_SOCKADDR_STRING_LEN];
    //int keepidle = 60; // seconds
    //int keepinterval = 10; // seconds
    //int keepcount = 5; // tries
    //int thread_return;
    int socket_rendezvous = -1;



    //PeerConnectionData peer_data;
    //ssize_t bytes_received;
    int enable_flag = 1;


    memset(saddr, 0, sizeof(*saddr));
    sa_in = (struct sockaddr_in*)saddr;
    atomic_store(&connection_established, false);
    atomic_store(&accepting_socket, -1);

    timeout.tv_sec = timeout_ms / 1000;
    timeout.tv_usec = (timeout_ms % 1000) * 1000;

    socket_rendezvous = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_rendezvous == -1) {
        ucs_error("Could not create socket for rendezvous server: ");
        return UCS_ERR_IO_ERROR;
    }

    // Enable binding multiple sockets to the same local endpoint, see https://bford.info/pub/net/p2pnat/ for details

    if (setsockopt(socket_rendezvous, SOL_SOCKET, SO_REUSEADDR, &enable_flag, sizeof(int)) < 0 ||
        setsockopt(socket_rendezvous, SOL_SOCKET, SO_REUSEPORT, &enable_flag, sizeof(int)) < 0) {
        ucs_error("Setting REUSE options failed: ");
        return UCS_ERR_IO_ERROR;
    }
    if (setsockopt(socket_rendezvous, SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeout, sizeof timeout) < 0 ||
        setsockopt(socket_rendezvous, SOL_SOCKET, SO_REUSEPORT, &enable_flag, sizeof(int)) < 0) {
        ucs_error("Setting timeout failed: ");
        return UCS_ERR_IO_ERROR;
    }


    server_data.sin_family = AF_INET;
    server_data.sin_addr.s_addr = inet_addr(server_address);
    server_data.sin_port = htons(port);

    if (connect(socket_rendezvous, (struct sockaddr *)&server_data, sizeof(server_data)) != 0) {
        ucs_error("Connection with the rendezvous server failed: ");
        return UCS_ERR_IO_ERROR;

    }

    if(send(socket_rendezvous, pairing_name, strlen(pairing_name), MSG_DONTWAIT) == -1) {
        ucs_error("Failed to send data to rendezvous server: ");
        return UCS_ERR_IO_ERROR;
    }


    bytes = recv(socket_rendezvous, &public_info, sizeof(public_info), MSG_WAITALL);
    if (bytes == -1) {
        ucs_error("Failed to get data from rendezvous server: ");
        return UCS_ERR_IO_ERROR;
    } else if(bytes == 0) {
        ucs_error("Server has disconnected");
        return UCS_ERR_IO_ERROR;
    }



    ucs_warn("client data: %s:%i", ip_to_string(&public_info.ip.s_addr, ipadd, sizeof(ipadd)), ntohs(public_info.port));

    sa_in->sin_family = AF_INET;
    sa_in->sin_addr.s_addr = INADDR_ANY;
    sa_in->sin_port = public_info.port;

    data->ip = public_info.ip;
    data->port = public_info.port;

    /*thread_return = pthread_create(&ping_thread, NULL, (void *)ping, (void*) pairing_name);
    if(thread_return) {
        ucs_error("Error when creating thread for listening: ");
        return UCS_ERR_IO_ERROR;
    }*/

    return UCS_OK;
}

int pair(int *peer_socket, struct sockaddr_storage *saddr, const char * pairing_name, const char * server_address, int port, int timeout_ms) {

    //struct sockaddr_in peer_addr;
    struct sockaddr_in *sa_in;
    struct timeval timeout;
    struct sockaddr_in server_data;
    PeerConnectionData public_info;
    ssize_t bytes;
    pthread_t peer_listen_thread;
    int thread_return;
    PeerConnectionData peer_data;
    ssize_t bytes_received;
    struct sockaddr_in local_port_addr;
    char ipadd[UCS_SOCKADDR_STRING_LEN];
    int enable_flag = 1;
    int peer_status;
    int flags;
    int fd;
    int socket_rendezvous = -1;

    memset(saddr, 0, sizeof(*saddr));
    sa_in = (struct sockaddr_in*)saddr;
    atomic_store(&connection_established, false);
    atomic_store(&accepting_socket, -1);

    timeout.tv_sec = timeout_ms / 1000;
    timeout.tv_usec = (timeout_ms % 1000) * 1000;



    socket_rendezvous = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_rendezvous == -1) {
        ucs_error("Could not create socket for rendezvous server: ");
        return UCS_ERR_IO_ERROR;
    }

    // Enable binding multiple sockets to the same local endpoint, see https://bford.info/pub/net/p2pnat/ for details

    if (setsockopt(socket_rendezvous, SOL_SOCKET, SO_REUSEADDR, &enable_flag, sizeof(int)) < 0 ||
        setsockopt(socket_rendezvous, SOL_SOCKET, SO_REUSEPORT, &enable_flag, sizeof(int)) < 0) {
        ucs_error("Setting REUSE options failed: ");
        return UCS_ERR_IO_ERROR;
    }
    if (setsockopt(socket_rendezvous, SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeout, sizeof timeout) < 0 ||
        setsockopt(socket_rendezvous, SOL_SOCKET, SO_REUSEPORT, &enable_flag, sizeof(int)) < 0) {
        ucs_error("Setting timeout failed: ");
        return UCS_ERR_IO_ERROR;
    }

    server_data.sin_family = AF_INET;
    server_data.sin_addr.s_addr = inet_addr(server_address);
    server_data.sin_port = htons(port);

    if (connect(socket_rendezvous, (struct sockaddr *)&server_data, sizeof(server_data)) != 0) {
        ucs_error("Connection with the rendezvous server failed: ");
        return UCS_ERR_IO_ERROR;

    }

    if(send(socket_rendezvous, pairing_name, strlen(pairing_name), MSG_DONTWAIT) == -1) {
        ucs_error("Failed to send data to rendezvous server: ");
        return UCS_ERR_IO_ERROR;
    }


    bytes = recv(socket_rendezvous, &public_info, sizeof(public_info), MSG_WAITALL);
    if (bytes == -1) {
        ucs_error("Failed to get data from rendezvous server: ");
        return UCS_ERR_IO_ERROR;
    } else if(bytes == 0) {
        ucs_error("Server has disconnected");
        return UCS_ERR_IO_ERROR;
    }


    thread_return = pthread_create(&peer_listen_thread, NULL, (void *)peer_listen, (void*) &public_info);
    if(thread_return) {
        ucs_error("Error when creating thread for listening: ");
        return UCS_ERR_IO_ERROR;
    }



    // Wait until rendezvous server sends info about peer
    bytes_received = recv(socket_rendezvous, &peer_data, sizeof(peer_data), MSG_WAITALL);
    if(bytes_received == -1) {
        ucs_error("Failed to get peer data from rendezvous server: ");
        return UCS_ERR_IO_ERROR;
    } else if(bytes_received == 0) {
        ucs_error("Server has disconnected when waiting for peer data");
        return UCS_ERR_IO_ERROR;
    }

    ucs_warn("Peer: %s:%i", ip_to_string(&peer_data.ip.s_addr, ipadd, sizeof(ipadd)), ntohs(peer_data.port));


    //We do NOT close the socket_rendez socket here, otherwise the next binds sometimes fail (although SO_REUSEADDR|SO_REUSEPORT is set)!

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enable_flag, sizeof(int)) < 0 ||
        setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &enable_flag, sizeof(int)) < 0) {
        ucs_error("Setting REUSE options failed");
        return UCS_ERR_IO_ERROR;
    }

    //Set socket to non blocking for the following polling operations
    if(fcntl(fd, F_SETFL, O_NONBLOCK) != 0) {
        ucs_error("Setting O_NONBLOCK failed: ");
        return UCS_ERR_IO_ERROR;
    }


    local_port_addr.sin_family = AF_INET;
    local_port_addr.sin_addr.s_addr = INADDR_ANY;
    local_port_addr.sin_port = public_info.port;

    if (bind(fd, (const struct sockaddr *)&local_port_addr, sizeof(local_port_addr))) {
        ucs_error("Binding to same port failed: ");
        return UCS_ERR_IO_ERROR;
    }


    sa_in->sin_family = AF_INET;
    sa_in->sin_addr.s_addr = peer_data.ip.s_addr;
    sa_in->sin_port = peer_data.port;

    while(!atomic_load(&connection_established)) {
        peer_status = connect(fd, (struct sockaddr *)saddr, sizeof(struct sockaddr));
        if (peer_status != 0) {
            if (errno == EALREADY || errno == EAGAIN || errno == EINPROGRESS) {
                continue;
            } else if(errno == EISCONN) {

                ucs_warn("Succesfully connected to peer, EISCONN");
                break;
            } else {

                msleep(100);
                //std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }
        } else {
            ucs_warn("Succesfully connected to peer, peer_status");
            break;
        }
    }

    if(atomic_load(&connection_established)) {
        pthread_join(peer_listen_thread, NULL);
        fd = atomic_load(&accepting_socket);
    }

    flags = fcntl(fd,  F_GETFL, 0);
    flags &= ~(O_NONBLOCK);
    fcntl(fd, F_SETFL, flags);

    ucs_warn("returning UCS_OK from tcpunch");

    *peer_socket = fd;

    return UCS_OK;
}
