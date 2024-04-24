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

ucs_status_t connectandBindLocal(int *rendezvous_fd, PeerConnectionData * data, struct sockaddr_storage *saddr,
                                 const char * pairing_name, const char* server_address, int port) {

  // call rendezvous and get public IP address
  // leave fd open so don't close until established connection with peer
  struct sockaddr_in *sa_in;
  //struct timeval timeout;
  struct sockaddr_in local_addr;
  uint16_t mapped_port;
  socklen_t local_addr_len = sizeof(local_addr);


  struct sockaddr_in server_data;
  PeerConnectionData public_info;
  ssize_t bytes;
  char ipadd[UCS_SOCKADDR_STRING_LEN];

  int enable_flag = 1;
  int fd, ret;
  ucs_status_t status;

  memset(&local_addr, 0, local_addr_len);
  sa_in = (struct sockaddr_in *)saddr;


 /* timeout.tv_sec = timeout_ms / 1000;
  timeout.tv_usec = (timeout_ms % 1000) * 1000;*/

  fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd == -1) {
    ucs_error("Could not create socket for rendezvous server: ");
    return UCS_ERR_IO_ERROR;
  }

  // Enable binding multiple sockets to the same local endpoint, see https://bford.info/pub/net/p2pnat/ for details

  if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enable_flag, sizeof(int)) <
          0 ||
      setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &enable_flag, sizeof(int)) <
          0) {
    ucs_error("Setting REUSE options failed: ");
    return UCS_ERR_IO_ERROR;
  }

  local_addr.sin_family = AF_INET;
  local_addr.sin_addr = sa_in->sin_addr;
  local_addr.sin_port = 0;
  /*if (setsockopt(*fd, SOL_SOCKET, SO_RCVTIMEO, (const char *)&timeout,
                 sizeof timeout) < 0) {
    ucs_error("Setting timeout failed: ");
    return UCS_ERR_IO_ERROR;
  }*/

  /*if (getsockname(*address_fd, (struct sockaddr*)&local_addr, &local_addr_len) < 0) {
    ucs_error("getsockname failed: ");
    return UCS_ERR_IO_ERROR;
  }*/



  if (bind(fd, (struct sockaddr*)&local_addr, local_addr_len) < 0) {
    ucs_error("error binding to rendezvous socket %s", strerror(errno));
    return UCS_ERR_IO_ERROR;
  }

  /* Get the port which was selected for the socket */
  ret = getsockname(fd, (struct sockaddr*)&local_addr, &local_addr_len);
  if (ret < 0) {
    ucs_error("getsockname(fd=%d) failed: %m", fd);
  }

  status = ucs_sockaddr_get_port((struct sockaddr*)&local_addr, &mapped_port);
  if (status != UCS_OK) {
    ucs_error("ucs_sockaddr_get_port failed");
  }

  ucs_warn("local address used to bind for rendezvous %s",
           ucs_sockaddr_str((struct sockaddr*)&local_addr,
                            ipadd, sizeof(ipadd)));


  server_data.sin_family = AF_INET;
  server_data.sin_addr.s_addr = inet_addr(server_address);
  server_data.sin_port = htons(port);

  if (connect(fd, (struct sockaddr *)&server_data, sizeof(server_data)) != 0) {
    ucs_error("Connection with the rendezvous server failed: %s", strerror(errno));
    return UCS_ERR_IO_ERROR;

  }


  if(send(fd, pairing_name, strlen(pairing_name), MSG_DONTWAIT) == -1) {
    ucs_error("Failed to send data to rendezvous server: ");
    return UCS_ERR_IO_ERROR;
  }


  bytes = recv(fd, &public_info, sizeof(public_info), MSG_WAITALL);
  if (bytes == -1) {
    ucs_error("Failed to get data from rendezvous server: ");
    return UCS_ERR_IO_ERROR;
  } else if(bytes == 0) {
    ucs_error("Server has disconnected");
    return UCS_ERR_IO_ERROR;
  }

  ucs_warn("client data returned from rendezvous: %s:%i", ip_to_string(&public_info.ip.s_addr, ipadd, sizeof(ipadd)),
           ntohs(public_info.port));

  if (ntohs(public_info.port) != mapped_port) {
    ucs_warn("public port %i does not match private port %i", ntohs(public_info.port), sa_in->sin_port);
  }

  sa_in->sin_family = local_addr.sin_family;
  sa_in->sin_addr = local_addr.sin_addr;
  sa_in->sin_port = local_addr.sin_port;


  data->ip = public_info.ip;
  data->port = public_info.port;

  *rendezvous_fd = fd;

  return UCS_OK;
}
