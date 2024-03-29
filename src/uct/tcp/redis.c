//
// Created by parallels on 3/29/24.
//


#include "redis.h"



void connectCallback(const redisAsyncContext *c, int status) {
    if (status != REDIS_OK) {
        ucs_warn("Async Connect Error: %s", c->errstr);
        return;
    }
    ucs_warn("Redis Async Connected...");
}

void setCallback(redisAsyncContext *c, void *r, void *privdata) {
    redisReply *reply = r;
    if (reply == NULL) return;
    printf("SET: %s\n", reply->str);
}


struct redisAsyncContext * redisAsyncLogin(const char * hostName, int port) {
    struct timeval tv = {0};
    redisOptions options = {0};
    redisAsyncContext *c = NULL;
    REDIS_OPTIONS_SET_TCP(&options, "127.0.0.1", 6379);

    tv.tv_sec = 1;
    options.connect_timeout = &tv;


    c = redisAsyncConnectWithOptions(&options);
    if (c->err) {
        /* Let *c leak for now... */
        ucs_warn("Error: %s", c->errstr);
        return NULL;
    }

    return c;
}

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

void setRedisValueAsync(const char *hostname, int port, const char *key, const char *value) {
    struct event_base *base = event_base_new();
    redisAsyncContext * asyncContext = redisAsyncLogin(hostname, port);

    if (base != NULL && asyncContext != NULL) {
        redisLibeventAttach(asyncContext,base);
        redisAsyncSetConnectCallback(asyncContext,connectCallback);

        redisAsyncCommand(asyncContext, setCallback, NULL, "SET %s %s", key, value);

        event_base_dispatch(base);

    }
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