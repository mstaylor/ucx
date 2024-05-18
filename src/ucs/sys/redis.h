//
// Created by parallels on 3/29/24.
//

#ifndef UCX_REDIS_H
#define UCX_REDIS_H

#include "hiredis/hiredis.h"


#include <uct/tcp/tcp.h>

#define PEER_KEY "peer"
#define PEER_KEY2 "peer2"

redisContext * redisLogin(const char *hostname, int port);


ucs_status_t setRedisValue(const char *hostname, int port, const char *key, const char *value);

ucs_status_t setRedisValueWithContext(redisContext *c, const char *key, const char *value);

ucs_status_t deleteRedisKey(const char *hostname, int port, const char *key);

ucs_status_t deleteRedisKeyTransactional(const char *hostname, int port, const char *key);
ucs_status_t deleteRedisKeyTransactionalithContext(redisContext *c, const char *key);

ucs_status_t updateKeyIfMissing(const char *hostname, int port, const char *key, const char *value);



char * getValueFromRedis(const char *hostname, int port, const char *key);

char * getValueFromRedisWithContext(redisContext *c, const char *key);





#endif //UCX_REDIS_H
