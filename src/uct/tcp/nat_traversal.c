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

_Noreturn void listen_for_updates(void *p) {
  char* remote_address = NULL;
  char src_str[UCS_SOCKADDR_STRING_LEN];
  char src_str2[UCS_SOCKADDR_STRING_LEN];
  char peer_redis_key[UCS_SOCKADDR_STRING_LEN*2];
  int fd, ret;
  int enable_flag = 1;
  struct sockaddr_in local_port_addr;
  socklen_t local_addr_len = sizeof(local_port_addr);
  uint16_t mapped_port;
  struct sockaddr_in server_data;
  PeerConnectionData public_info;
  ssize_t bytes;
  char source_ipadd[UCS_SOCKADDR_STRING_LEN];
  char public_ipadd[UCS_SOCKADDR_STRING_LEN];
  char * token = NULL;
  int token_index = 0;
  char publicAddress[UCS_SOCKADDR_STRING_LEN];
  char publicAddressPort[UCS_SOCKADDR_STRING_LEN*2];
  int publicPort = 0;

  struct sockaddr_storage connect_addr;
  struct sockaddr* addr = NULL;

  size_t addrlen;
  size_t addr_len;

  int peer_fd;
  struct timeval timeout;
  int retries = 0;
  int result = 0;
  fd_set set;
  int so_error;
  socklen_t len = sizeof(so_error);
  int flags;

  uct_tcp_iface_t *iface = (uct_tcp_iface_t *)p;
  struct sockaddr_in *sa_in = (struct sockaddr_in  *)&iface->config.ifaddr;
  ucs_sockaddr_str((struct sockaddr *)&iface->config.ifaddr,
                   src_str, sizeof(src_str));


  sprintf(peer_redis_key, "%s:%s", PEER_KEY, src_str);
  ucs_warn("retrieving key->value from redis - key: %s", peer_redis_key);
  while(true) { //loop throughout the lifetime of the ucx process
    //1. poll redis for the
    remote_address = getValueFromRedis(iface->config.redis_ip_address,
                                       iface->config.redis_port, src_str);
    while (remote_address == NULL) {
      msleep(1000);
      // ucs_warn("sleeping waiting for remote address from redis...");
      remote_address =
          getValueFromRedis(iface->config.redis_ip_address,
                            iface->config.redis_port, peer_redis_key);
    }

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


    //1. Call Rendezvous server and retrieve public port
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

    if (getsockname(iface->listen_fd, (struct sockaddr*)&local_port_addr, &local_addr_len)< 0 ) {
      ucs_warn("getsockname failed");
      continue;
    }

    if (bind(fd, (struct sockaddr*)&local_port_addr, local_addr_len) < 0) {
      ucs_error("error binding to rendezvous socket %s", strerror(errno));
      continue;
    }

    ret = getsockname(fd, (struct sockaddr*)&local_port_addr, &local_addr_len);
    if (ret < 0) {
      ucs_error("getsockname(fd=%d) failed: %m", fd);
    }

    if (ucs_sockaddr_get_port((struct sockaddr*)&local_port_addr, &mapped_port) != UCS_OK) {
      ucs_error("ucs_sockaddr_get_port failed");
      continue;
    }

    ucs_warn("local address used to bind for rendezvous %s",
             ucs_sockaddr_str((struct sockaddr*)&local_port_addr,
                              source_ipadd, sizeof(source_ipadd)));

    server_data.sin_family = AF_INET;
    server_data.sin_addr.s_addr = inet_addr(iface->config.rendezvous_ip_address);
    server_data.sin_port = htons(iface->config.rendezvous_port);

    if (connect(fd, (struct sockaddr *)&server_data, sizeof(server_data)) != 0) {
      ucs_error("Connection with the rendezvous server failed: %s", strerror(errno));
      continue;
    }

    if(send(fd, iface->config.pairing_name, strlen(iface->config.pairing_name),
             MSG_DONTWAIT) == -1) {
      ucs_error("Failed to send data to rendezvous server: ");
      continue;
    }


    bytes = recv(fd, &public_info, sizeof(public_info), MSG_WAITALL);
    if (bytes == -1) {
      ucs_error("Failed to get data from rendezvous server: ");
      continue;
    } else if(bytes == 0) {
      ucs_error("Server has disconnected");
      continue;
    }

    ucs_warn("client data returned from rendezvous: %s:%i",
             ip_to_string(&public_info.ip.s_addr,
                          public_ipadd,
                          sizeof(public_ipadd)),
             ntohs(public_info.port));

    if (ntohs(public_info.port) != mapped_port) {
      ucs_warn("public port %i does not match private port %i", ntohs(public_info.port), sa_in->sin_port);
    }

    sprintf(publicAddressPort, "%s:%i", public_ipadd, ntohs(public_info.port));

    //2. write redis value
    setRedisValue(iface->config.redis_ip_address, iface->config.redis_port,
                  source_ipadd, publicAddressPort);
    ucs_warn("wrote redis key:value %s:%s", source_ipadd, public_ipadd);

    timeout.tv_sec = NAT_CONNECT_TO_SEC;
    timeout.tv_usec = 0;

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


    if(ucs_socket_setopt(peer_fd, SOL_SOCKET, SO_SNDTIMEO,
                               &timeout,
                               sizeof timeout) != UCS_OK) {
          ucs_warn("could NOT configure to set connect timeout");
          continue;
    }


    if (ucs_sockaddr_sizeof((struct sockaddr *)&local_port_addr, &addr_len) != UCS_OK) {
      ucs_warn("ucs_sockaddr_sizeof failed ");
      continue;
    }


    ret = bind(peer_fd, (struct sockaddr *)&local_port_addr, addr_len);
    if (ret < 0) {

      ucs_warn("bind(fd=%d addr=%s) failed: %m",
               peer_fd, ucs_sockaddr_str((struct sockaddr *)&local_port_addr,
                                    src_str2, sizeof(src_str2)));
      continue;
    }
    ucs_sockaddr_str((struct sockaddr *)&local_port_addr,
                     src_str2, sizeof(src_str2));

    ucs_warn("bound peer endpoint socket ip: %s", src_str2);

    set_sock_addr(publicAddress, &connect_addr, AF_INET, publicPort);

    addr = (struct sockaddr*)&connect_addr;

    if(ucs_sockaddr_sizeof(addr, &addrlen) != UCS_OK) {
      ucs_warn("ucs_sockaddr_sizeof failed");
      continue;
    }

    if(fcntl(peer_fd, F_SETFL, O_NONBLOCK) != 0) {
      ucs_warn("Setting O_NONBLOCK failed: ");
      continue;
    }

    retries = 0;
    //next ping the peer a few times to try to connect


    while (retries < NAT_RETRIES) {
      ucs_warn("retrying connection - current retry: %i", retries);


      ucs_sockaddr_str(addr,
                       src_str2, sizeof(src_str2));

      ucs_warn("connecting to peer address socket ip: %s", src_str2);

      result = connect(peer_fd, addr, addrlen);

      if (result == 0) {
        ucs_warn("successfully connected to peer: %s", src_str2);
        break;
      }

      FD_ZERO(&set);
      FD_SET(peer_fd, &set);

      result = select(peer_fd + 1, NULL, &set, NULL, &timeout);
      if (result <= 0) {
        // select() failed or connection timed out
        ucs_warn("select failed/connect timeout on peer socket %i", peer_fd);
      } else {
        getsockopt(peer_fd, SOL_SOCKET, SO_ERROR, &so_error, &len);
        if (so_error == 0) {
          ucs_warn("Connected on attempt %d", retries + 1);

          break;
        } else {
          ucs_warn("Connection failed: %s and continuing", strerror(so_error));
        }
      }

      close(peer_fd);

      if (ucs_socket_create(AF_INET, SOCK_STREAM, &peer_fd) != UCS_OK) {
        ucs_warn("could not create socket");
        break;
      }


      if(ucs_socket_setopt(peer_fd, SOL_SOCKET, SO_REUSEPORT,
                                 &enable_flag, sizeof(int))!= UCS_OK) {
        ucs_warn("could NOT configure to reuse socket port");
        break;
      }


      if (ucs_socket_setopt(peer_fd, SOL_SOCKET, SO_REUSEADDR,
                                 &enable_flag, sizeof(int)) != UCS_OK) {
        ucs_warn("could NOT configure to reuse socket address");
        break;
      }

      if (ucs_socket_setopt(peer_fd, SOL_SOCKET, SO_SNDTIMEO,
                                 &timeout,
                                 sizeof timeout) != UCS_OK) {
        ucs_warn("could NOT configure to set connect timeout");
        break;
      }

      if (ucs_sockaddr_sizeof((struct sockaddr *)&local_port_addr, &addr_len) != UCS_OK) {

        ucs_warn("ucs_sockaddr_sizeof failed ");
        break;
      }

      ret = bind(peer_fd, (struct sockaddr *)&local_port_addr, addr_len);
      if (ret < 0) {

        ucs_warn("bind(fd=%d addr=%s) failed: %m",
                 fd, ucs_sockaddr_str((struct sockaddr *)&local_port_addr,
                                      src_str2, sizeof(src_str2)));
        continue;
      }


      ucs_sockaddr_str((struct sockaddr *)&local_port_addr,
                       src_str2, sizeof(src_str2));

      ucs_warn("bound endpoint socket ip: %s", src_str2);

      if(fcntl(peer_fd, F_SETFL, O_NONBLOCK) != 0) {
        ucs_warn("Setting O_NONBLOCK failed: ");
        continue;
      }

      retries++;
    }

    flags = fcntl(peer_fd,  F_GETFL, 0);
    flags &= ~(O_NONBLOCK);
    fcntl(peer_fd, F_SETFL, flags);

    //delete redis key
    deleteRedisKey(iface->config.redis_ip_address, iface->config.redis_port, src_str);

  }



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
