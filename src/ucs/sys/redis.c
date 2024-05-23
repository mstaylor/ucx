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



ucs_status_t setRedisValueWithContext(redisContext *c, const char *key, const char *value) {
  redisReply *reply;
  ucs_status_t status = UCS_OK;


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

  }

  return status;
}

pthread_mutex_t redis_set_mutex;

ucs_status_t  setRedisValue(const char *hostname, int port, const char *key, const char *value) {



    redisReply *reply;
    ucs_status_t status = UCS_OK;
    redisContext *c = NULL;
    pthread_mutex_lock(&redis_set_mutex);
    c = redisLogin(hostname, port);

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

    pthread_mutex_unlock(&redis_set_mutex);

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

ucs_status_t deleteRedisKeyTransactionalithContext(redisContext *c, const char *key) {
  ucs_status_t status = UCS_OK;

  redisReply *reply;

  if (c != NULL) {

    reply = redisCommand(c,"WATCH %s", key);
    if (reply == NULL) {
      ucs_warn("Failed to execute WATCH command");
      return UCS_ERR_IO_ERROR;
    }
    freeReplyObject(reply);

    reply = redisCommand(c,"GET %s", key);

    if (reply == NULL) {
      //key exists so not updating
      return UCS_ERR_IO_ERROR;
    }
    freeReplyObject(reply);

    reply = redisCommand(c,"MULTI");
    if (reply == NULL) {
      ucs_warn("could not set redis MULTI");
      return UCS_ERR_IO_ERROR;
    }
    freeReplyObject(reply);
    ucs_warn("deleting redis key %s", key);
    reply = redisCommand(c, "DEL %s", key);
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

    reply = redisCommand(c,"EXEC");
    if (reply->type == REDIS_REPLY_ARRAY) {
      ucs_warn("Transaction executed, counter delete key: %s ", key);
      status = UCS_OK;
    } else {
      printf("Transaction failed, counter not delete Key: %s ", key);
      status = UCS_ERR_IO_ERROR;
    }
    freeReplyObject(reply);

  }


  return status;
}


ucs_status_t deleteRedisKeyTransactional(const char *hostname, int port, const char *key) {
  ucs_status_t status = UCS_OK;

  redisReply *reply;
  redisContext *c = redisLogin(hostname, port);

  if (c != NULL) {

    reply = redisCommand(c,"WATCH %s", key);
    if (reply == NULL) {
      ucs_warn("Failed to execute WATCH command");
      return UCS_ERR_IO_ERROR;
    }
    freeReplyObject(reply);

    reply = redisCommand(c,"GET %s", key);

    if (reply == NULL) {
      //key exists so not updating
      return UCS_ERR_IO_ERROR;
    }
    freeReplyObject(reply);

    reply = redisCommand(c,"MULTI");
    if (reply == NULL) {
      ucs_warn("could not set redis MULTI");
      return UCS_ERR_IO_ERROR;
    }
    freeReplyObject(reply);
    ucs_warn("deleting redis key %s", key);
    reply = redisCommand(c, "DEL %s", key);
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

    reply = redisCommand(c,"EXEC");
    if (reply->type == REDIS_REPLY_ARRAY) {
      ucs_warn("Transaction executed, counter delete key: %s ", key);
      status = UCS_OK;
    } else {
      printf("Transaction failed, counter not delete Key: %s ", key);
      status = UCS_ERR_IO_ERROR;
    }
    freeReplyObject(reply);

  }


  return status;
}

ucs_status_t updateKeyIfMissingWithContext(redisContext *c, const char *key, const char *value) {
  ucs_status_t status = UCS_OK;

  redisReply *reply;


  if (c != NULL) {

    reply = redisCommand(c,"WATCH %s", key);
    if (reply == NULL) {
      ucs_warn("Failed to execute WATCH command");
      return UCS_ERR_IO_ERROR;
    }
    freeReplyObject(reply);

    reply = redisCommand(c,"GET %s", key);

    if (reply == NULL) {
      //key exists so not updating
      return UCS_ERR_IO_ERROR;
    }
    freeReplyObject(reply);

    reply = redisCommand(c,"MULTI");
    if (reply == NULL) {
      ucs_warn("could not set redis MULTI");
      return UCS_ERR_IO_ERROR;
    }
    freeReplyObject(reply);

    reply = redisCommand(c,"SET %s %s", key, value);
    if (reply == NULL) {
      ucs_warn("could not set value in redis transaction");
      return UCS_ERR_IO_ERROR;
    }
    freeReplyObject(reply);

    reply = redisCommand(c,"EXEC");
    if (reply->type == REDIS_REPLY_ARRAY) {
      ucs_warn("Transaction executed, counter updated key: %s value: %s", key, value);
      status = UCS_OK;
    } else {
      printf("Transaction failed, counter not updated Key: %s value: %s", key, value);
      status = UCS_ERR_IO_ERROR;
    }
    freeReplyObject(reply);

  }


  return status;
}

ucs_status_t updateKeyIfMissing(const char *hostname, int port, const char *key, const char *value) {
  ucs_status_t status = UCS_OK;

  redisReply *reply;
  redisContext *c = redisLogin(hostname, port);

  if (c != NULL) {

    reply = redisCommand(c,"WATCH %s", key);
    if (reply == NULL) {
      ucs_warn("Failed to execute WATCH command");
      return UCS_ERR_IO_ERROR;
    }
    freeReplyObject(reply);

    reply = redisCommand(c,"GET %s", key);

    if (reply == NULL) {
      //key exists so not updating
      return UCS_ERR_IO_ERROR;
    }
    freeReplyObject(reply);

    reply = redisCommand(c,"MULTI");
    if (reply == NULL) {
      ucs_warn("could not set redis MULTI");
      return UCS_ERR_IO_ERROR;
    }
    freeReplyObject(reply);

    reply = redisCommand(c,"SET %s %s", key, value);
    if (reply == NULL) {
      ucs_warn("could not set value in redis transaction");
      return UCS_ERR_IO_ERROR;
    }
    freeReplyObject(reply);

    reply = redisCommand(c,"EXEC");
    if (reply->type == REDIS_REPLY_ARRAY) {
      ucs_warn("Transaction executed, counter updated key: %s value: %s", key, value);
      status = UCS_OK;
    } else {
      printf("Transaction failed, counter not updated Key: %s value: %s", key, value);
      status = UCS_ERR_IO_ERROR;
    }
    freeReplyObject(reply);

  }


  return status;


}

char * getValueFromRedisWithContext(redisContext *c, const char *key) {
  redisReply *reply;

  char * result = NULL;

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

  }
  return result;
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