//
// Created by parallels on 3/29/24.
//


#include "redis.h"






redisContext * redisLogin(const char *hostname, int port) {
    redisContext *c;
    ucs_warn("Logging into redis host: %s port: %i", hostname, port);

    // Connect to Redis server
    c = redisConnect(hostname, port);
    if (c == NULL || c->err) {
        if (c) {
            ucs_warn("Connection error: %s\n", c->errstr);
            redisFree(c);
        } else {
            ucs_warn("Connection error: can't allocate redis context\n");
        }
        return NULL;
    }

    return c;

}




void setRedisValue(const char *hostname, int port, const char *key, const char *value) {

    redisReply *reply;

    redisContext *c = redisLogin(hostname, port);

    if (c != NULL) {
        // Set the key
        reply = redisCommand(c, "SET %s %s", key, value);
        if (reply == NULL) {
            ucs_warn("Error in SET command\n");
        } else {
            // Print the reply
            ucs_warn("%s\n", reply->str);
            // Free the reply object
            freeReplyObject(reply);
        }

        // Disconnect from Redis
        redisFree(c);
    }
}

char * getValueFromRedis(const char *hostname, int port, const char *key){
    redisReply *reply;

    char * result = NULL;

    redisContext *c = redisLogin(hostname, port);

    if (c != NULL) {
        reply = redisCommand(c, "GET %s", key);
        if (reply == NULL) {
            ucs_warn("Error in GET command or key not found\n");
        } else {
            // store the value in a char*
            if (reply->type == REDIS_REPLY_STRING) {
                ucs_warn("The value of '%s' is: %s\n", key, reply->str);
                result = (char*) malloc(strlen(reply->str)+1);
                strcpy(result, reply->str);

            } else {
                ucs_warn("The key '%s' does not exist\n", key);
            }
            freeReplyObject(reply);
        }

        // Disconnect from Redis
        redisFree(c);
    }
    return result;
}