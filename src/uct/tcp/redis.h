//
// Created by parallels on 3/29/24.
//

#ifndef UCX_REDIS_H
#define UCX_REDIS_H

#include "hiredis/hiredis.h"


#include "tcp.h"

redisContext * redisLogin(const char *hostname, int port);


void setRedisValue(const char *hostname, int port, const char *key, const char *value);



char * getValueFromRedis(const char *hostname, int port, const char *key);

#endif //UCX_REDIS_H
