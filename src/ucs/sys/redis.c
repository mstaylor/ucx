//
// Created by parallels on 3/29/24.
//

#include "redis.h"
#include <stdbool.h>
#include <time.h>

// Seed the random number generator
void initialize_random() {
  static bool initialized = false;
  if (!initialized) {
    srand(time(NULL));
    initialized = true;
  }
}

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

ucs_status_t writeRedisHashValue(const char * hostname, int port, const char *hash, const char* key, const char* value) {
  redisReply *reply;
  ucs_status_t status = UCS_OK;
  redisContext *c = NULL;
  pthread_mutex_lock(&redis_set_mutex);
  c = redisLogin(hostname, port);

  if (c != NULL) {
    // Set the key
    reply = redisCommand(c, "HSET %s %s %s", hash, key, value);
    if (reply == NULL) {
      ucs_warn("Error in SET command");
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

void generate_random_string(char *str, size_t length) {
  // Define the character set
  const char charset[] =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  size_t charset_size = sizeof(charset) - 1;

  initialize_random();

  // Generate the random string
  for (size_t i = 0; i < length; i++) {
    int key = rand() % charset_size;
    str[i] = charset[key];
  }

  // Null-terminate the string
  str[length] = '\0';
}



char * retrieveKeyAndUpdateKeyIfMissing(const char *hostname, int port, const char *key1, const char *key2) {

  char *result = NULL;
  char randomString[UCS_SOCKADDR_STRING_LEN * 2];
  char pair_value[200];

  redisReply *reply = NULL;
  redisContext *c = redisLogin(hostname, port);

  if (c != NULL) {

    reply = redisCommand(c,"WATCH %s %s", key1, key2);
    if (reply == NULL) {
      ucs_warn("Failed to execute WATCH command");
      return result;
    }
    freeReplyObject(reply);

    ucs_warn("retrieving key: %s from redis", key1);
    reply = redisCommand(c,"GET %s", key1);

    if (reply == NULL || reply->type != REDIS_REPLY_STRING) {
      //key exists so not updating
      ucs_warn("retrieving key: %s from redis", key2);
      reply = redisCommand(c,"GET %s", key2);
    }
    //we return the pair value since it has been set
    if (reply != NULL && reply->type == REDIS_REPLY_STRING) {
      ucs_warn("returning the pair value since it has been previously set");
      result = (char*) malloc(strlen(reply->str)+1);
      strcpy(result, reply->str);
      freeReplyObject(reply);
      return result;
    }

    reply = redisCommand(c,"MULTI");
    if (reply == NULL) {
      ucs_warn("could not set redis MULTI");
      return result;
    }
    freeReplyObject(reply);


    // Generate random string for unique pair name
    generate_random_string(randomString, UCS_SOCKADDR_STRING_LEN);

    sprintf(pair_value, "%s:%s", PAIR, randomString);

    ucs_warn("random string: %s pair_value:%s", randomString, pair_value);

    reply = redisCommand(c,"SET %s %s", key1, pair_value);
    if (reply == NULL) {
      ucs_warn("could not set value in redis transaction");
      return result;
    }
    freeReplyObject(reply);

    reply = redisCommand(c,"EXEC");
    if (reply->type == REDIS_REPLY_ARRAY) {
      ucs_warn("Transaction executed, counter updated key: %s value: %s", key1, pair_value);
      result = (char*) malloc(strlen(pair_value)+1);
      strcpy(result, pair_value);

    } else {
      ucs_warn("Transaction failed, counter not updated Key: %s value: %s", key1, pair_value);

    }
    freeReplyObject(reply);

  } else {
    ucs_warn("could not connect to redis...");
  }



  return result;


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

bool redisHashKeyExists(const char * hostname, int port, const char *hash, const char* key) {

  bool result = false;
  redisReply *reply;

  //char * result = NULL;

  redisContext *c = redisLogin(hostname, port);

  if (c != NULL) {
    reply = redisCommand(c, "HGET %s %s", hash, key);
    if (reply == NULL) {
      ucs_warn("Error in HGET command or key not found");
    } else {
      // store the value in a char*
      if (reply->type == REDIS_REPLY_STRING) {
        result = true;
        //ucs_warn("The value of '%s' is: %s", key, reply->str);
        //result = (char*) malloc(strlen(reply->str)+1);
        //strcpy(result, reply->str);

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