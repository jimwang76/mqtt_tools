#include <execinfo.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <memory.h>
#include <unistd.h>
#include <time.h>

#include <mosquitto.h>
#include <mysql/my_global.h>
#include <mysql/mysql.h>

enum
{
    LOG_LEVEL_DEFAULT       = 0,
    LOG_LEVEL_MQTT          = 1,
    LOG_LEVEL_MQTT_MSG      = 2,
    LOG_LEVEL_MYSQL         = 3,
    LOG_LEVEL_TOPICS        = 4,
    LOG_LEVEL_NEW_TOPICS    = 5,
};

static unsigned int s_log_level = ((1 << LOG_LEVEL_DEFAULT) | (1 << LOG_LEVEL_MQTT) | (1 << LOG_LEVEL_MYSQL) | (1 << LOG_LEVEL_NEW_TOPICS));

#define DEBUG_PRINT(level, ...)     if (s_log_level & (1 << level)) { printf(__VA_ARGS__); fflush(stdout); }

#define DEBUG_DEFAULT(...)      DEBUG_PRINT(LOG_LEVEL_DEFAULT, __VA_ARGS__)
#define DEBUG_MQTT(...)         DEBUG_PRINT(LOG_LEVEL_MQTT, __VA_ARGS__)
#define DEBUG_MYSQL(...)        DEBUG_PRINT(LOG_LEVEL_MYSQL, __VA_ARGS__)
#define DEBUG_MQTT_MSG(...)     DEBUG_PRINT(LOG_LEVEL_MQTT_MSG, __VA_ARGS__)
#define DEBUG_TOPICS(...)       DEBUG_PRINT(LOG_LEVEL_TOPICS, __VA_ARGS__)
#define DEBUG_NEW_TOPICS(...)   DEBUG_PRINT(LOG_LEVEL_NEW_TOPICS, __VA_ARGS__)

typedef struct
{
    int id;
    const char *topic;
} topic_t;

topic_t *topics = NULL;
int num_topics = 0;

#define db_host "localhost"
#define db_username "pi"
#define db_password "linuxroot"
#define db_database "mqttdb"
#define db_port 0

#define db_insert_history   "INSERT INTO history (timestamp, topic_id, value) VALUES (?,?,?)"
#define db_insert_topic     "INSERT INTO topics (topic) VALUES (?)"

#define mqtt_host "localhost"
#define mqtt_port 1883

static int run = 1;
static MYSQL *connection = NULL;
static MYSQL_STMT *history_insert_stmt = NULL;

inline unsigned int get_time_ms()
{
    struct timespec cur;
    clock_gettime(CLOCK_MONOTONIC, &cur);
    return cur.tv_sec * 1000 + cur.tv_nsec / 1000000;
}

void handle_signal(int s)
{
    run = 0;
}

void connect_callback(struct mosquitto *mosq, void *obj, int result)
{
}

int load_topic_id(void)
{
    MYSQL_RES *result;
    MYSQL_ROW row;
    
    if (mysql_query(connection, "SELECT * FROM topics")) 
    {
        DEBUG_MYSQL("%s\n", mysql_error(connection));
        return 1;
    }
    
    result = mysql_store_result(connection);
    if (result == NULL) 
    {
        DEBUG_MYSQL("%s\n", mysql_error(connection));
        return 1;
    }
    
    DEBUG_NEW_TOPICS("------------------------------------\n");
    while ((row = mysql_fetch_row(result))) 
    {
        topics = (topic_t *)realloc(topics, sizeof(topic_t) * (num_topics + 1));
        topics[num_topics].id = atoi(row[0]);
        topics[num_topics].topic = strdup(row[1]);
        DEBUG_NEW_TOPICS("[%d] %s\n", topics[num_topics].id, topics[num_topics].topic);
        num_topics ++;
    }
    
    mysql_free_result(result);
    
    DEBUG_NEW_TOPICS("------------------------------------\n");
    return 0;
}

topic_t *get_topic(int id)
{
    int i;
    for (i = 0; i < num_topics; i ++)
    {
        if (topics[i].id == id)
        {
            return &topics[i];
        }
    }
    return NULL;
}

int get_topic_id(const char *topic)
{
    int i, topic_id = -1;
    
    for (i = 0; i < num_topics; i ++)
    {
        if (!strcmp(topic, topics[i].topic))
        {
            topic_id = topics[i].id;
            break;
        }
    }
    
    if (topic_id == -1)
    {
        MYSQL_STMT *topic_stmt;
        MYSQL_BIND bind[1];
        memset(bind, 0, sizeof(bind));
        
        bind[0].buffer_type = MYSQL_TYPE_STRING;
        bind[0].buffer = (char *)topic;
        bind[0].buffer_length= strlen(topic);
        bind[0].is_null= 0;
        
        topic_stmt = mysql_stmt_init(connection);
        mysql_stmt_prepare(topic_stmt, db_insert_topic, strlen(db_insert_topic));
        mysql_stmt_bind_param(topic_stmt, bind);
        mysql_stmt_execute(topic_stmt);
        topic_id = mysql_insert_id(connection);
        
        topics = (topic_t *)realloc(topics, sizeof(topic_t) * (num_topics + 1));
        topics[num_topics].id = topic_id;
        topics[num_topics].topic = strdup(topic);
        DEBUG_NEW_TOPICS("New topic %d [%s]\n", topics[num_topics].id, topics[num_topics].topic);
        num_topics ++;
    }
    
    return topic_id;
}

void message_callback(struct mosquitto *mosq, void *obj, const struct mosquitto_message *message)
{
    if (!strncmp(message->topic, "/home/history", 13)) return;
    if (!strncmp(message->topic, "/home/notification", 18)) return;
    if (!strncmp(message->topic, "/home/devicestatus", 18)) return;
    if (strstr(message->topic, "history")) return;
    
    MYSQL_BIND bind[3];
    int topic_id;
    time_t current_time;
    int time_sec;
    
    topic_id = get_topic_id(message->topic);
    current_time = time(NULL);
    time_sec = current_time;
    DEBUG_MQTT_MSG("%8d [%d] %s : %s\n", time_sec, topic_id, message->topic, (char *)message->payload);
    
#if 1
    memset(bind, 0, sizeof(bind));
    bind[0].buffer_type = MYSQL_TYPE_LONG;
    bind[0].buffer = &time_sec;
    bind[0].buffer_length= 0;
    bind[0].is_null= 0;
    bind[1].buffer_type = MYSQL_TYPE_LONG;
    bind[1].buffer = &topic_id;
    bind[1].buffer_length= 0;
    bind[1].is_null= 0;
    bind[2].buffer_type = MYSQL_TYPE_STRING;
    bind[2].buffer = message->payload;
    bind[2].buffer_length= strlen(message->payload);
    bind[2].is_null= 0;
    
    mysql_stmt_bind_param(history_insert_stmt, bind);
    mysql_stmt_execute(history_insert_stmt);
#endif
}

int main(int argc, char **argv)
{
    my_bool reconnect = true;
    char clientid[24];
    struct mosquitto *mosq;
    int rc = 0;
    int c;
    
    DEBUG_MQTT("MySQL client version: %s\n", mysql_get_client_info());

    while ((c = getopt(argc, argv, "l:")) != -1)
    {
        switch (c)
        {
        case 'l':
            s_log_level = atoi(optarg);
            break;
        default:
            break;
        }
    }

    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);
    
    mysql_library_init(0, NULL, NULL);
    mosquitto_lib_init();
    
    connection = mysql_init(NULL);
    
    if (connection == NULL) 
    {
        DEBUG_MYSQL("%s\n", mysql_error(connection));
        exit(1);
    }
    
    mysql_options(connection, MYSQL_OPT_RECONNECT, &reconnect);
    
    connection = mysql_real_connect(connection, db_host, db_username, db_password, db_database, db_port, NULL, 0);
    
    if(connection)
    {
        load_topic_id();
        
        history_insert_stmt = mysql_stmt_init(connection);
        mysql_stmt_prepare(history_insert_stmt, db_insert_history, strlen(db_insert_history));
        
        memset(clientid, 0, 24);
        snprintf(clientid, 23, "mysql_log_%d", getpid());
        mosq = mosquitto_new(clientid, true, connection);
        
        if(mosq)
        {
            mosquitto_connect_callback_set(mosq, connect_callback);
            mosquitto_message_callback_set(mosq, message_callback);
            
            rc = mosquitto_connect(mosq, mqtt_host, mqtt_port, 60);
            
            mosquitto_subscribe(mosq, NULL, "/home/#", 0);
            
            while(run)
            {
                rc = mosquitto_loop(mosq, -1, 1);
                if(run && rc)
                {
                    sleep(20);
                    mosquitto_reconnect(mosq);
                }
            }
            mosquitto_destroy(mosq);
        }
        mysql_stmt_close(history_insert_stmt);
        
        mysql_close(connection);
    }
    else
    {
        DEBUG_MQTT("Error: Unable to connect to database.\n");
        DEBUG_MQTT("%s\n", mysql_error(connection));
        rc = 1;
    }
    
    mysql_library_end();
    mosquitto_lib_cleanup();
    
    return rc;
}
