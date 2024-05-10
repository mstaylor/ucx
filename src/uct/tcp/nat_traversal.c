//
// Created by parallels on 3/28/24.
//

#include "nat_traversal.h"
#include "redis.h"
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>

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

void listen_for_updates_peer2(void *p) {
  char* remote_address = NULL;
  char src_str[UCS_SOCKADDR_STRING_LEN];
  char src_str2[UCS_SOCKADDR_STRING_LEN];
  char peer_redis_key[UCS_SOCKADDR_STRING_LEN*2];
  //int fd = -1, ret;
  int enable_flag = 1;
  struct sockaddr_in local_port_addr;
  struct sockaddr_in local_port_addr2;
  socklen_t local_addr_len = sizeof(local_port_addr);
  socklen_t local_addr_len2 = sizeof(local_port_addr2);
  //uint16_t mapped_port;
  //struct sockaddr_in server_data;
  //PeerConnectionData public_info;
  //ssize_t bytes;
  //char source_ipadd[UCS_SOCKADDR_STRING_LEN];
  //char public_ipadd[UCS_SOCKADDR_STRING_LEN];
  char * token = NULL;
  int token_index = 0;
  char publicAddress[UCS_SOCKADDR_STRING_LEN];
  char publicAddressPort[UCS_SOCKADDR_STRING_LEN*2];
  int publicPort = 0;
  ucs_status_t redis_write_status;

  struct sockaddr_storage connect_addr;
  struct sockaddr* addr = NULL;
  //char src_str2[UCS_SOCKADDR_STRING_LEN];
  size_t addrlen;
  //size_t addr_len;

  int peer_fd;
  struct timeval timeout;
  int retries = 0;
  int result = 0;
  int result_opt;
  fd_set set;
  int so_error;
  socklen_t len = sizeof(so_error);

  ucs_status_t status;


  uct_tcp_iface_t *iface = (uct_tcp_iface_t *)p;

  ucs_sockaddr_str((struct sockaddr *)&iface->config.ifaddr,
                   src_str, sizeof(src_str));


  sprintf(peer_redis_key, "%s:%s", PEER_KEY2, src_str);
  ucs_warn("retrieving key->value from redis - key: %s", peer_redis_key);

  while(true) { //loop throughout the lifetime of the ucx process
    //1. poll redis for the
    remote_address = getValueFromRedis(iface->config.redis_ip_address,
                                       iface->config.redis_port, peer_redis_key);


    while (remote_address == NULL) {
      msleep(6000);
      // ucs_warn("sleeping waiting for remote address from redis...");
      remote_address =
          getValueFromRedis(iface->config.redis_ip_address,
                            iface->config.redis_port, peer_redis_key);
    }
    ucs_warn("received remote address: %s", remote_address);
    //peer endpoint should now start pinging the source endpoint
    token = strtok(remote_address, ":");

    while (token != NULL) {
      if (token_index == 0) {
        strcpy(publicAddress, token);
      } else if (token_index == 1){
        publicPort = atoi(token);
      }

      token = strtok(NULL, ":");
      token_index++;
    }

    ucs_warn("tokenized public address to %s and port %i from redis", publicAddress, publicPort);

    free(remote_address);
    remote_address = NULL;

    if (getsockname(iface->listen_fd, (struct sockaddr *)&local_port_addr,
                    &local_addr_len) < 0) {
      ucs_warn("getsockname failed");
      continue;
    }



    sprintf(publicAddressPort, "%s:%i", iface->config.public_ip_address,
            ntohs(local_port_addr.sin_port));



    //write redis value (private->public and public->public)
    redis_write_status = setRedisValue(iface->config.redis_ip_address, iface->config.redis_port,
                                       src_str, publicAddressPort);
    if (redis_write_status == UCS_OK) {
      ucs_warn("wrote redis private to public key:value %s->%s", src_str, publicAddressPort);
    } else {
      ucs_warn("could not write redis private to public key:value %s->%s", src_str, publicAddressPort);
    }

    redis_write_status = setRedisValue(iface->config.redis_ip_address, iface->config.redis_port,
                                       publicAddressPort, publicAddressPort);
    if (redis_write_status == UCS_OK) {
      ucs_warn("wrote redis public to public key:value %s->%s", publicAddressPort, publicAddressPort);
    } else {
      ucs_warn("could not write redis public to public key:value %s->%s", publicAddressPort, publicAddressPort);
    }


    ucs_warn("deleting redis key: %s", peer_redis_key);
    //delete redis key
    deleteRedisKeyTransactional(iface->config.redis_ip_address, iface->config.redis_port, peer_redis_key);

    set_sock_addr(publicAddress, &connect_addr, AF_INET, publicPort);

    addr = (struct sockaddr*)&connect_addr;

    if(ucs_sockaddr_sizeof(addr, &addrlen) != UCS_OK) {
      ucs_warn("ucs_sockaddr_sizeof failed");
      continue;
    }

    if (ucs_socket_create(AF_INET, SOCK_STREAM, &peer_fd) != UCS_OK) {
      ucs_warn("could not create socket");
      continue;
    }


    if (ucs_socket_setopt(peer_fd, SOL_SOCKET, SO_REUSEPORT,
                          &enable_flag, sizeof(int)) != UCS_OK) {
      ucs_warn("could NOT configure to reuse socket port");
      continue;

    }


    if (ucs_socket_setopt(peer_fd, SOL_SOCKET, SO_REUSEADDR,
                          &enable_flag, sizeof(int)) != UCS_OK) {
      ucs_warn("could NOT configure to reuse socket address");
      continue;
    }

    local_port_addr2.sin_family = AF_INET;
    local_port_addr2.sin_addr.s_addr = INADDR_ANY;
    local_port_addr2.sin_port = local_port_addr.sin_port;

    if (bind(peer_fd, (struct sockaddr *)&local_port_addr2, local_addr_len2) < 0) {
      ucs_error("error binding to rendezvous socket %s", strerror(errno));
      continue;
    }

    if(fcntl(peer_fd, F_SETFL, O_NONBLOCK) != 0) {
      ucs_warn("Setting O_NONBLOCK failed: ");
    }
    timeout.tv_sec = NAT_CONNECT_TO_SEC;
    timeout.tv_usec = 0;
    while (retries < NAT_RETRIES) {
      ucs_warn("retrying connection - current retry: %i", retries);

      ucs_sockaddr_str(addr,
                       src_str2, sizeof(src_str2));

      ucs_warn("connecting to peer address socket ip: %s", src_str2);

      result = connect(peer_fd, addr, addrlen);

      if (result == 0) {
        status = UCS_OK;
        break;
      }

      FD_ZERO(&set);
      FD_SET(peer_fd, &set);
      timeout.tv_sec = 10; // 10 second timeout
      timeout.tv_usec = 0;

      result = select(peer_fd + 1, NULL, &set, NULL, &timeout);
      if (result < 0 && errno != EINTR) {
        // select() failed or connection timed out
        ucs_warn("select failed/connect timeout on peer socket %i", peer_fd);
      }  else if (result > 0) {
        result_opt = getsockopt(peer_fd, SOL_SOCKET, SO_ERROR, &so_error, &len);
        if (result_opt < 0) {
          ucs_warn("Connection failed: %s and continuing", strerror(so_error));
        } else if (so_error) {
          ucs_warn("Error in delayed connection() %d - %s", so_error, strerror(so_error));
        } else {
          ucs_warn("Connected on attempt %d", retries + 1);
          status = UCS_OK;
          //close(fd);//close the rendezvous socket
          break;
        }
      } else {
        ucs_warn("Timeout or error.");
      }

      close(peer_fd);

      status = ucs_socket_create(AF_INET, SOCK_STREAM, &peer_fd);
      if (status != UCS_OK) {
        ucs_warn("could not create socket");
        break;
      }


      status = ucs_socket_setopt(peer_fd, SOL_SOCKET, SO_REUSEPORT,
                                 &enable_flag, sizeof(int));
      if (status != UCS_OK) {
        ucs_warn("could NOT configure to reuse socket port");
        break;
      }


      status = ucs_socket_setopt(peer_fd, SOL_SOCKET, SO_REUSEADDR,
                                 &enable_flag, sizeof(int));
      if (status != UCS_OK) {
        ucs_warn("could NOT configure to reuse socket address");
        break;
      }

      if (bind(peer_fd, (struct sockaddr *)&local_port_addr2, local_addr_len2) < 0) {
        ucs_error("error binding to rendezvous socket %s", strerror(errno));
        continue;
      }



      if(fcntl(peer_fd, F_SETFL, O_NONBLOCK) != 0) {
        ucs_warn("Setting O_NONBLOCK failed: ");
      }

      retries++;
    }

  }
}

void listen_for_updates_peer(void *p) {
  char* remote_address = NULL;
  char src_str[UCS_SOCKADDR_STRING_LEN];
  char src_str2[UCS_SOCKADDR_STRING_LEN];
  char peer_redis_key[UCS_SOCKADDR_STRING_LEN*2];
  //int fd = -1, ret;
  int enable_flag = 1;
  struct sockaddr_in local_port_addr;
  struct sockaddr_in local_port_addr2;
  socklen_t local_addr_len = sizeof(local_port_addr);
  socklen_t local_addr_len2 = sizeof(local_port_addr2);
  //uint16_t mapped_port;
  //struct sockaddr_in server_data;
  //PeerConnectionData public_info;
  //ssize_t bytes;
  //char source_ipadd[UCS_SOCKADDR_STRING_LEN];
  //char public_ipadd[UCS_SOCKADDR_STRING_LEN];
  char * token = NULL;
  int token_index = 0;
  char publicAddress[UCS_SOCKADDR_STRING_LEN];
  char publicAddressPort[UCS_SOCKADDR_STRING_LEN*2];
  int publicPort = 0;

  struct sockaddr_storage connect_addr;
  struct sockaddr* addr = NULL;
  //char src_str2[UCS_SOCKADDR_STRING_LEN];
  size_t addrlen;
  //size_t addr_len;

  int peer_fd;
  struct timeval timeout;
  int retries = 0;
  int result = 0;
  int result_opt;
  fd_set set;
  int so_error;
  socklen_t len = sizeof(so_error);
  ucs_status_t redis_write_status;
  ucs_status_t status;
  //int flags;

  uct_tcp_iface_t *iface = (uct_tcp_iface_t *)p;
  //struct sockaddr_in *sa_in = (struct sockaddr_in  *)&iface->config.ifaddr;
  ucs_sockaddr_str((struct sockaddr *)&iface->config.ifaddr,
                   src_str, sizeof(src_str));


  sprintf(peer_redis_key, "%s:%s", PEER_KEY, src_str);
  ucs_warn("retrieving key->value from redis - key: %s", peer_redis_key);

  while(true) { //loop throughout the lifetime of the ucx process
    //1. poll redis for the
    remote_address = getValueFromRedis(iface->config.redis_ip_address,
                                       iface->config.redis_port, peer_redis_key);


    while (remote_address == NULL) {
      msleep(6000);
      // ucs_warn("sleeping waiting for remote address from redis...");
      remote_address =
          getValueFromRedis(iface->config.redis_ip_address,
                            iface->config.redis_port, peer_redis_key);
    }
    ucs_warn("received remote address: %s", remote_address);
    //peer endpoint should now start pinging the source endpoint
    token = strtok(remote_address, ":");

    while (token != NULL) {
      if (token_index == 0) {
        strcpy(publicAddress, token);
      } else if (token_index == 1){
        publicPort = atoi(token);
      }

      token = strtok(NULL, ":");
      token_index++;
    }

    ucs_warn("tokenized public address to %s and port %i from redis", publicAddress, publicPort);

    free(remote_address);
    remote_address = NULL;

    if (getsockname(iface->listen_fd, (struct sockaddr *)&local_port_addr,
                    &local_addr_len) < 0) {
      ucs_warn("getsockname failed");
      continue;
    }

    local_port_addr2.sin_family = AF_INET;
    local_port_addr2.sin_addr.s_addr = INADDR_ANY;
    local_port_addr2.sin_port = local_port_addr.sin_port;

    sprintf(publicAddressPort, "%s:%i", iface->config.public_ip_address,
            ntohs(local_port_addr.sin_port));


    //1. Call Rendezvous server and retrieve public port if we don't already have done so
    /*if (!(strlen(publicAddressPort)> 0)) {
      ucs_warn("calling rendezvous within nat_traversal");
      fd = socket(AF_INET, SOCK_STREAM, 0);
      if (fd == -1) {
        ucs_error("Could not create socket for rendezvous server: ");
        continue;
      }

      if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enable_flag, sizeof(int)) <
              0 ||
          setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &enable_flag, sizeof(int)) <
              0) {
        ucs_error("Setting REUSE options failed: ");
        continue;
      }

      if (getsockname(iface->listen_fd, (struct sockaddr *)&local_port_addr,
                      &local_addr_len) < 0) {
        ucs_warn("getsockname failed");
        continue;
      }

      local_port_addr2.sin_family = AF_INET;
      local_port_addr2.sin_addr.s_addr = INADDR_ANY;
      local_port_addr2.sin_port = local_port_addr.sin_port;

      if (bind(fd, (struct sockaddr *)&local_port_addr2, local_addr_len2) < 0) {
        ucs_error("error binding to rendezvous socket %s", strerror(errno));
        continue;
      }

      ret = getsockname(fd, (struct sockaddr *)&local_port_addr2,
                        &local_addr_len2);
      if (ret < 0) {
        ucs_error("getsockname(fd=%d) failed: %m", fd);
      }

      if (ucs_sockaddr_get_port((struct sockaddr *)&local_port_addr2,
                                &mapped_port) != UCS_OK) {
        ucs_error("ucs_sockaddr_get_port failed");
        continue;
      }

      ucs_warn("local address used to bind for rendezvous %s",
               ucs_sockaddr_str((struct sockaddr *)&local_port_addr2,
                                source_ipadd, sizeof(source_ipadd)));

      server_data.sin_family = AF_INET;
      server_data.sin_addr.s_addr =
          inet_addr(iface->config.rendezvous_ip_address);
      server_data.sin_port = htons(iface->config.rendezvous_port);

      while (connect(fd, (struct sockaddr *)&server_data,
                     sizeof(server_data)) != 0) {
        ucs_error("Connection with the rendezvous server failed: %s",
                  strerror(errno));
        msleep(1000);
      }

      if (send(fd, iface->config.pairing_name,
               strlen(iface->config.pairing_name), MSG_DONTWAIT) == -1) {
        ucs_error("Failed to send data to rendezvous server: ");
        continue;
      }

      bytes = recv(fd, &public_info, sizeof(public_info), MSG_WAITALL);
      if (bytes == -1) {
        ucs_error("Failed to get data from rendezvous server: ");
        continue;
      } else if (bytes == 0) {
        ucs_error("Server has disconnected");
        continue;
      }

      close(fd);

      ucs_warn("client data returned from rendezvous: %s:%i",
               ip_to_string(&public_info.ip.s_addr, public_ipadd,
                            sizeof(public_ipadd)),
               ntohs(public_info.port));

      if (ntohs(public_info.port) != mapped_port) {
        ucs_warn("public port %i does not match private port %i",
                 ntohs(public_info.port), sa_in->sin_port);
      }

      sprintf(publicAddressPort, "%s:%i", public_ipadd,
              ntohs(public_info.port));
    }*/

    //write redis value (private->public and public->public)
    redis_write_status = setRedisValue(iface->config.redis_ip_address, iface->config.redis_port,
                                       src_str, publicAddressPort);
    if (redis_write_status == UCS_OK) {
      ucs_warn("wrote redis private to public key:value %s->%s", src_str, publicAddressPort);
    } else {
      ucs_warn("could not write redis private to public key:value %s->%s", src_str, publicAddressPort);
    }

    redis_write_status = setRedisValue(iface->config.redis_ip_address, iface->config.redis_port,
                                       publicAddressPort, publicAddressPort);
    if (redis_write_status == UCS_OK) {
      ucs_warn("wrote redis public to public key:value %s->%s", publicAddressPort, publicAddressPort);
    } else {
      ucs_warn("could not write redis public to public key:value %s->%s", publicAddressPort, publicAddressPort);
    }

    ucs_warn("deleting redis key: %s", peer_redis_key);
    //delete redis key
    deleteRedisKey(iface->config.redis_ip_address, iface->config.redis_port, peer_redis_key);

    set_sock_addr(publicAddress, &connect_addr, AF_INET, publicPort);

    addr = (struct sockaddr*)&connect_addr;

    if(ucs_sockaddr_sizeof(addr, &addrlen) != UCS_OK) {
      ucs_warn("ucs_sockaddr_sizeof failed");
      continue;
    }

    if (ucs_socket_create(AF_INET, SOCK_STREAM, &peer_fd) != UCS_OK) {
      ucs_warn("could not create socket");
      continue;
    }


    if (ucs_socket_setopt(peer_fd, SOL_SOCKET, SO_REUSEPORT,
                          &enable_flag, sizeof(int)) != UCS_OK) {
      ucs_warn("could NOT configure to reuse socket port");
      continue;

    }


    if (ucs_socket_setopt(peer_fd, SOL_SOCKET, SO_REUSEADDR,
                          &enable_flag, sizeof(int)) != UCS_OK) {
      ucs_warn("could NOT configure to reuse socket address");
      continue;
    }

    if (bind(peer_fd, (struct sockaddr *)&local_port_addr2, local_addr_len2) < 0) {
      ucs_error("error binding to peer socket error %s", strerror(errno));
      continue;
    }

    if(fcntl(peer_fd, F_SETFL, O_NONBLOCK) != 0) {
      ucs_warn("Setting O_NONBLOCK failed: ");
    }
    timeout.tv_sec = NAT_CONNECT_TO_SEC;
    timeout.tv_usec = 0;
    while (retries < NAT_RETRIES) {
      ucs_warn("retrying connection - current retry: %i", retries);

      ucs_sockaddr_str(addr,
                       src_str2, sizeof(src_str2));

      ucs_warn("connecting to peer address socket ip: %s", src_str2);

      result = connect(peer_fd, addr, addrlen);

      if (result == 0) {
        status = UCS_OK;
        break;
      }

      FD_ZERO(&set);
      FD_SET(peer_fd, &set);
      timeout.tv_sec = 10; // 10 second timeout
      timeout.tv_usec = 0;

      result = select(peer_fd + 1, NULL, &set, NULL, &timeout);
      if (result < 0 && errno != EINTR) {
        // select() failed or connection timed out
        ucs_warn("select failed/connect timeout on peer socket %i", peer_fd);
      }  else if (result > 0) {
        result_opt = getsockopt(peer_fd, SOL_SOCKET, SO_ERROR, &so_error, &len);
        if (result_opt < 0) {
          ucs_warn("Connection failed: %s and continuing", strerror(so_error));
        } else if (so_error) {
          ucs_warn("Error in delayed connection() %d - %s", so_error, strerror(so_error));
        } else {
          ucs_warn("Connected on attempt %d", retries + 1);
          status = UCS_OK;
          //close(fd);//close the rendezvous socket
          break;
        }
      } else {
        ucs_warn("Timeout or error.");
      }

      close(peer_fd);

      status = ucs_socket_create(AF_INET, SOCK_STREAM, &peer_fd);
      if (status != UCS_OK) {
        ucs_warn("could not create socket");
        break;
      }


      status = ucs_socket_setopt(peer_fd, SOL_SOCKET, SO_REUSEPORT,
                                 &enable_flag, sizeof(int));
      if (status != UCS_OK) {
        ucs_warn("could NOT configure to reuse socket port");
        break;
      }


      status = ucs_socket_setopt(peer_fd, SOL_SOCKET, SO_REUSEADDR,
                                 &enable_flag, sizeof(int));
      if (status != UCS_OK) {
        ucs_warn("could NOT configure to reuse socket address");
        break;
      }

      if (bind(peer_fd, (struct sockaddr *)&local_port_addr2, local_addr_len2) < 0) {
        ucs_error("error binding to rendezvous socket %s", strerror(errno));
        continue;
      }



      if(fcntl(peer_fd, F_SETFL, O_NONBLOCK) != 0) {
        ucs_warn("Setting O_NONBLOCK failed: ");
      }

      retries++;
    }

  }



}

