//
// Created by parallels on 3/29/24.
//


#include "redis.h"


redisContext * redisLogin(const char *hostname, int port) {
    redisContext *c;
//    ucs_warn("Logging into redis host: %s port: %i", hostname, port);

    // Connect to Redis server
    c = redisConnect(hostname, port);
    if (c == NULL || c->err) {
        if (c) {
            ucs_warn("Connection error: %s", c->errstr);
            redisFree(c);
        } else {
            ucs_warn("Connection error: can't allocate redis context\n");
        }
        return NULL;
    }

    return c;

}




ucs_status_t  setRedisValue(const char *hostname, int port, const char *key, const char *value) {

    redisReply *reply;
    ucs_status_t status = UCS_OK;

    redisContext *c = redisLogin(hostname, port);

    if (c != NULL) {
        // Set the key
        reply = redisCommand(c, "SET %s %s", key, value);
        if (reply == NULL) {
            ucs_warn("Error in SET command\n");
            status = UCS_ERR_IO_ERROR;
        } else {
            // Print the reply
            //ucs_warn("%s\n", reply->str);
            // Free the reply object
            freeReplyObject(reply);
        }

        // Disconnect from Redis
        redisFree(c);
    }

    return status;
}

ucs_status_t deleteRedisKey(const char *hostname, int port, const char *key) {
    ucs_status_t status = UCS_OK;

    redisContext *c = redisLogin(hostname, port);

    if (c != NULL) {
      // Deleting the key using DEL command
      redisReply *reply = redisCommand(c, "DEL %s", key);
      if (reply->type == REDIS_REPLY_INTEGER) {
        if (reply->integer == 1) {
          ucs_warn("Key '%s' deleted successfully.", key);
        } else {
          ucs_warn("Key '%s' does not exist.", key);
        }
      } else {
        ucs_warn("Error: %s", c->errstr);
        status = UCS_ERR_IO_ERROR;
      }
      freeReplyObject(reply);

      // Disconnect and free context
      redisFree(c);
    }
    return status;
}
char * getValueFromRedis(const char *hostname, int port, const char *key){
    redisReply *reply;

    char * result = NULL;

    redisContext *c = redisLogin(hostname, port);

    if (c != NULL) {
        reply = redisCommand(c, "GET %s", key);
        if (reply == NULL) {
            ucs_warn("Error in GET command or key not found");
        } else {
            // store the value in a char*
            if (reply->type == REDIS_REPLY_STRING) {
                //ucs_warn("The value of '%s' is: %s", key, reply->str);
                result = (char*) malloc(strlen(reply->str)+1);
                strcpy(result, reply->str);

            } //else {
                //ucs_warn("The key '%s' does not exist", key);
            //}
            freeReplyObject(reply);
        }

        // Disconnect from Redis
        redisFree(c);
    }
    return result;
}