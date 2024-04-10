//
// Created by parallels on 3/28/24.
//

#include "nat_traversal.h"
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


const char * ip_to_string(in_addr_t *ip, char * buffer, size_t max_size) {
    return inet_ntop(AF_INET, ip, buffer, max_size);

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

ucs_status_t connectandBindLocal(int *fd, PeerConnectionData * data, struct sockaddr_storage *saddr,
                                 const char * pairing_name, const char* server_address, int port, int timeout_ms) {

  //call rendezvous and get public IP address
  //leave fd open so don't close until established connection with peer
  struct sockaddr_in *sa_in;
  struct timeval timeout;
  struct sockaddr_in local_addr;
  socklen_t local_addr_len = sizeof(local_addr);

  struct sockaddr_in server_data;
  PeerConnectionData public_info;
  ssize_t bytes;
  char ipadd[UCS_SOCKADDR_STRING_LEN];

  int enable_flag = 1;


  //memset(saddr, 0, sizeof(*saddr));
  sa_in = (struct sockaddr_in*)saddr;

  timeout.tv_sec = timeout_ms / 1000;
  timeout.tv_usec = (timeout_ms % 1000) * 1000;

  *fd = socket(AF_INET, SOCK_STREAM, 0);
  if (*fd == -1) {
    ucs_error("Could not create socket for rendezvous server: ");
    return UCS_ERR_IO_ERROR;
  }

  // Enable binding multiple sockets to the same local endpoint, see https://bford.info/pub/net/p2pnat/ for details

  if (setsockopt(*fd, SOL_SOCKET, SO_REUSEADDR, &enable_flag, sizeof(int)) < 0 ||
      setsockopt(*fd, SOL_SOCKET, SO_REUSEPORT, &enable_flag, sizeof(int)) < 0) {
    ucs_error("Setting REUSE options failed: ");
    return UCS_ERR_IO_ERROR;
  }
  if (setsockopt(*fd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeout, sizeof timeout) < 0 ||
      setsockopt(*fd, SOL_SOCKET, SO_REUSEPORT, &enable_flag, sizeof(int)) < 0) {
    ucs_error("Setting timeout failed: ");
    return UCS_ERR_IO_ERROR;
  }

  server_data.sin_family = AF_INET;
  server_data.sin_addr.s_addr = inet_addr(server_address);
  server_data.sin_port = htons(port);

  if (connect(*fd, (struct sockaddr *)&server_data, sizeof(server_data)) != 0) {
    ucs_error("Connection with the rendezvous server failed: ");
    return UCS_ERR_IO_ERROR;

  }

  if (getsockname(*fd, (struct sockaddr *)&local_addr, &local_addr_len) == -1) {
    ucs_warn("Error getting local address");

  }

  // Print local port
  ucs_warn("Local ip/port: %s:%d sent to rendezvous",
           ip_to_string((in_addr_t *)&local_addr.sin_addr.s_addr, ipadd,sizeof(ipadd)),
                               ntohs(local_addr.sin_port));

  if(send(*fd, pairing_name, strlen(pairing_name), MSG_DONTWAIT) == -1) {
    ucs_error("Failed to send data to rendezvous server: ");
    return UCS_ERR_IO_ERROR;
  }


  bytes = recv(*fd, &public_info, sizeof(public_info), MSG_WAITALL);
  if (bytes == -1) {
    ucs_error("Failed to get data from rendezvous server: ");
    return UCS_ERR_IO_ERROR;
  } else if(bytes == 0) {
    ucs_error("Server has disconnected");
    return UCS_ERR_IO_ERROR;
  }

  ucs_warn("client data returned from rendezvous: %s:%i", ip_to_string(&public_info.ip.s_addr, ipadd, sizeof(ipadd)),
           ntohs(public_info.port));

  sa_in->sin_family = AF_INET;
  sa_in->sin_addr.s_addr = local_addr.sin_addr.s_addr;
  sa_in->sin_port = public_info.port;

  data->ip = public_info.ip;
  data->port = public_info.port;

  return UCS_OK;
}
