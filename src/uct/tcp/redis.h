//
// Created by parallels on 3/29/24.
//

#ifndef UCX_REDIS_H
#define UCX_REDIS_H

#include "hiredis/hiredis.h"
#include "hiredis/async.h"
#include <signal.h>
#include <event.h>
#include "hiredis/adapters/libevent.h"

#include "tcp.h"

redisContext * redisLogin(const char *hostname, int port);
redisAsyncContext * redisAsyncLogin(const char * hostName, int port);

void setRedisValue(const char *hostname, int port, const char *key, const char *value);

void setRedisValueAsync(const char *hostname, int port, const char *key, const char *value);

char * getValueFromRedis(const char *hostname, int port, const char *key);

#endif //UCX_REDIS_H
