//
// Created by parallels on 3/28/24.
//

#include "nat_traversal.h"
#include "ucs/sys/redis.h"
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

const char *ip_to_string(in_addr_t *ip, char *buffer, size_t max_size) {
  return inet_ntop(AF_INET, ip, buffer, max_size);
}

/* msleep(): Sleep for the requested number of milliseconds. */
int msleep(long msec) {
  struct timespec ts;
  int res;

  if (msec < 0) {
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

void generate_random_string(char *str, size_t length) {
  // Define the character set
  const char charset[] =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  size_t charset_size = sizeof(charset) - 1;

  // Seed the random number generator
  srand(time(NULL));

  // Generate the random string
  for (size_t i = 0; i < length; i++) {
    int key = rand() % charset_size;
    str[i] = charset[key];
  }

  // Null-terminate the string
  str[length] = '\0';
}

void listen_for_updates_peer(void *p) {
  char *remote_address = NULL;
  char src_str[UCS_SOCKADDR_STRING_LEN];
  char src_str2[UCS_SOCKADDR_STRING_LEN];
  char peer_redis_key[UCS_SOCKADDR_STRING_LEN * 2];
  char pair_key[UCS_SOCKADDR_STRING_LEN * 2];
  char pair_value[200];

  int fd = -1, ret;
  int enable_flag = 1;
  struct sockaddr_in local_port_addr;
  struct sockaddr_in local_port_addr2;
  socklen_t local_addr_len = sizeof(local_port_addr);
  socklen_t local_addr_len2 = sizeof(local_port_addr2);
  uint16_t mapped_port;
  struct sockaddr_in server_data;
  PeerConnectionData public_info;
  PeerConnectionData peer_data;
  ssize_t bytes;
  char source_ipadd[UCS_SOCKADDR_STRING_LEN];
  char public_ipadd[UCS_SOCKADDR_STRING_LEN];
  char publicAddressPort[UCS_SOCKADDR_STRING_LEN * 2];
  char randomString[UCS_SOCKADDR_STRING_LEN * 2];
  int publicPort = 0;

  struct sockaddr_storage connect_addr;
  struct sockaddr *addr = NULL;

  size_t addrlen;

  int public_port = -1;
  int peer_fd;
  struct timeval timeout;
  int retries = 0;
  int result = 0;
  int result_opt;
  fd_set set;
  int so_error;


  const char *peer_str = NULL;
  socklen_t len = sizeof(so_error);

  ucs_status_t status;

  uct_tcp_iface_t *iface = (uct_tcp_iface_t *)p;

  ucs_sockaddr_str((struct sockaddr *)&iface->config.ifaddr, src_str,
                   sizeof(src_str));

  ucs_warn("starting nat traversal thread for address: %s", src_str);

  if (getsockname(iface->listen_fd, (struct sockaddr *)&local_port_addr,
                  &local_addr_len) < 0) {
    ucs_warn("getsockname failed");
  }


  sprintf(peer_redis_key, "%s:%s", PEER_KEY2, src_str);


  // Generate random string for unique pair name
  generate_random_string(randomString, UCS_SOCKADDR_STRING_LEN);

  sprintf(pair_value, "%s:%s", PAIR, randomString);
  sprintf(pair_key, "%s_%s", src_str, PAIR);

  ucs_warn("writing pair_key: %s and pair_value:%s for src:%s", pair_key, pair_value, src_str);

  setRedisValue(iface->config.redis_ip_address, iface->config.redis_port,
                pair_key, pair_value);


  // Retrieve remote address written during endpoint start (cm_start)
  peer_str = peer_redis_key;

  while (true) { // loop throughout the lifetime of the ucx process
    // 1. poll redis for the


    remote_address = getValueFromRedis(iface->config.redis_ip_address,
                                       iface->config.redis_port, peer_str);

    while (remote_address == NULL) {
      msleep(1000);

      remote_address = getValueFromRedis(iface->config.redis_ip_address,
                                         iface->config.redis_port, peer_str);
    }

    ucs_warn("received peer address: %s for peer %s from redis", remote_address,
             peer_str);
    //we con't care about the remote_address since we're using the rendezvous address
    free(remote_address);
    remote_address = NULL;

    //now call rendez
    ucs_warn("calling rendezvous within nat_traversal");

    memset(&public_info, 0, sizeof(public_info));
    memset(&peer_data, 0, sizeof(peer_data));
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

    //set source prt to be the same as listen port

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

    ret =
        getsockname(fd, (struct sockaddr *)&local_port_addr2, &local_addr_len2);
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

    while (connect(fd, (struct sockaddr *)&server_data, sizeof(server_data)) !=
           0) {
      ucs_error("Connection with the rendezvous server failed: %s",
                strerror(errno));
      msleep(1000);
    }

    //use created pair value for pair name
    if (send(fd, pair_value, strlen(pair_value), MSG_DONTWAIT) == -1) {
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

    // Wait until rendezvous server sends info about peer
    bytes = recv(fd, &peer_data, sizeof(peer_data), MSG_WAITALL);
    if(bytes == -1) {
      ucs_warn("Failed to get peer data from rendezvous server: ");
      continue;
    } else if(bytes == 0) {
      ucs_warn("Server has disconnected when waiting for peer data");
      continue;
    }

    public_port = ntohs(peer_data.port);

    ucs_warn("client data returned from rendezvous: %s:%i for pair_value: %s",
             ip_to_string(&peer_data.ip.s_addr, public_ipadd,
                          sizeof(public_ipadd)),
             public_port, pair_value);

    //peer port is peer returned by rendezvous

    sprintf(publicAddressPort, "%s:%i", public_ipadd, public_port);

    //set connect_addr to address returned by rendez

    set_sock_addr(public_ipadd, &connect_addr, AF_INET, publicPort);

    addr = (struct sockaddr *)&connect_addr;

    if (ucs_sockaddr_sizeof(addr, &addrlen) != UCS_OK) {
      ucs_warn("ucs_sockaddr_sizeof failed");
      continue;
    }

    if (ucs_socket_create(AF_INET, SOCK_STREAM, &peer_fd) != UCS_OK) {
      ucs_warn("could not create socket");
      continue;
    }

    if (ucs_socket_setopt(peer_fd, SOL_SOCKET, SO_REUSEPORT, &enable_flag,
                          sizeof(int)) != UCS_OK) {
      ucs_warn("could NOT configure to reuse socket port");
      continue;
    }

    if (ucs_socket_setopt(peer_fd, SOL_SOCKET, SO_REUSEADDR, &enable_flag,
                          sizeof(int)) != UCS_OK) {
      ucs_warn("could NOT configure to reuse socket address");
      continue;
    }

    local_port_addr2.sin_family = AF_INET;
    local_port_addr2.sin_addr.s_addr = INADDR_ANY;
    local_port_addr2.sin_port = local_port_addr.sin_port;

    if (bind(peer_fd, (struct sockaddr *)&local_port_addr2, local_addr_len2) <
        0) {
      ucs_error("error binding to rendezvous socket %s", strerror(errno));
      continue;
    }

    if (fcntl(peer_fd, F_SETFL, O_NONBLOCK) != 0) {
      ucs_warn("Setting O_NONBLOCK failed: ");
    }
    timeout.tv_sec = NAT_CONNECT_TO_SEC;
    timeout.tv_usec = 0;
    while (retries < NAT_RETRIES) {
      ucs_warn("retrying connection - current retry: %i for %s", retries,
               src_str);

      ucs_sockaddr_str(addr, src_str2, sizeof(src_str2));

      ucs_warn("connecting to peer address socket ip: %s source str: %s for %s",
               src_str2, peer_str, src_str);

      result = connect(peer_fd, addr, addrlen);

      if (result == 0) {
        if (iface->config.enable_dummy_send) {
          ucs_warn("sending dummy payload...");
          sendTestMessage(peer_fd);
        }
        break;
      }

      FD_ZERO(&set);
      FD_SET(peer_fd, &set);
      timeout.tv_sec = 10; // 10 second timeout
      timeout.tv_usec = 0;

      result = select(peer_fd + 1, NULL, &set, NULL, &timeout);
      if (result < 0 && errno != EINTR) {
        // select() failed or connection timed out
        ucs_warn("select failed/connect timeout on peer socket %i peer address "
                 "%s source %s for %s",
                 peer_fd, src_str2, peer_str, src_str);
      } else if (result > 0) {
        result_opt = getsockopt(peer_fd, SOL_SOCKET, SO_ERROR, &so_error, &len);
        if (result_opt < 0) {
          ucs_warn("Connection failed: %s and continuing peer socket %i peer "
                   "address %s source %s for %s",
                   strerror(so_error), peer_fd, src_str2, peer_str, src_str);
        } else if (so_error) {
          ucs_warn("Error in delayed connection() %d - %s peer socket %i peer "
                   "address %s source str %s for %s",
                   so_error, strerror(so_error), peer_fd, src_str2, peer_str,
                   src_str);
        } else {
          ucs_warn("Connected on attempt %d peer socket %i peer address %s "
                   "source str %s for %s",
                   retries + 1, peer_fd, src_str2, peer_str, src_str);

          break;
        }
      } else {
        ucs_warn("Timeout or error. peer socket %i peer address %s for %s",
                 peer_fd, src_str2, src_str);
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

      if (bind(peer_fd, (struct sockaddr *)&local_port_addr2, local_addr_len2) <
          0) {
        ucs_error("error binding to rendezvous socket %s", strerror(errno));
        continue;
      }

      if (fcntl(peer_fd, F_SETFL, O_NONBLOCK) != 0) {
        ucs_warn("Setting O_NONBLOCK failed: ");
      }

      retries++;
    }

    //delete the peer key, since we've processed it

    ucs_warn("deleting redis key: %s for source %s for %s", peer_redis_key,
             peer_str, src_str);
    // delete redis key
    deleteRedisKey(iface->config.redis_ip_address, iface->config.redis_port,
                   peer_redis_key);

    // close(peer_fd);

    retries = 0;
  }
}

void sendTestMessage(int fd) {
  char buffer[200] = "Hello, server!";
  ssize_t bytes_sent;

  int retries = 6;

  int currentTry = 0;
  while (currentTry < retries) {
    bytes_sent = send(fd, buffer, strlen(buffer), 0);
    if (bytes_sent < 0) {
      if (errno == EPIPE) {
        perror("Broken pipe error");
        ucs_warn("could not send message");
        currentTry++;
        continue;
      } else {
        perror("Send error");
        ucs_warn("unable to send test message");
        currentTry++;
        continue;
      }
    } else {
      ucs_warn("Message sent: %s", buffer);
      break;
    }
  }
}
