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
//#include "zlib.h"
#include <minizip/zip.h>

enum
{
    LOG_LEVEL_DEFAULT       = 0,
    LOG_LEVEL_MQTT          = 1,
    LOG_LEVEL_MQTT_MSG      = 2,
    LOG_LEVEL_MYSQL         = 3,
    LOG_LEVEL_TOPICS        = 4,
    LOG_LEVEL_UPDATE        = 5,
    LOG_LEVEL_COLLECT       = 6,
    LOG_LEVEL_JSON          = 7,
    LOG_LEVEL_ZIP           = 8,
    LOG_LEVEL_PURGE_PERIOD  = 9,
    LOG_LEVEL_PURGE         = 10,
};

static unsigned int s_log_level = ((1 << LOG_LEVEL_DEFAULT) | (1 << LOG_LEVEL_MQTT) | (1 << LOG_LEVEL_MYSQL) | (1 << LOG_LEVEL_PURGE_PERIOD) | (1 << LOG_LEVEL_PURGE));

#define DEBUG_PRINT(level, ...)     if (s_log_level & (1 << level)) { printf(__VA_ARGS__); fflush(stdout); }

#define DEBUG_DEFAULT(...)      DEBUG_PRINT(LOG_LEVEL_DEFAULT, __VA_ARGS__)
#define DEBUG_MQTT(...)         DEBUG_PRINT(LOG_LEVEL_MQTT, __VA_ARGS__)
#define DEBUG_MYSQL(...)        DEBUG_PRINT(LOG_LEVEL_MYSQL, __VA_ARGS__)
#define DEBUG_MQTT_MSG(...)     DEBUG_PRINT(LOG_LEVEL_MQTT_MSG, __VA_ARGS__)
#define DEBUG_TOPICS(...)       DEBUG_PRINT(LOG_LEVEL_TOPICS, __VA_ARGS__)
#define DEBUG_UPDATE(...)       DEBUG_PRINT(LOG_LEVEL_UPDATE, __VA_ARGS__)
#define DEBUG_COLLECT(...)      DEBUG_PRINT(LOG_LEVEL_COLLECT, __VA_ARGS__)
#define DEBUG_JSON(...)         DEBUG_PRINT(LOG_LEVEL_JSON, __VA_ARGS__)
#define DEBUG_ZIP(...)          DEBUG_PRINT(LOG_LEVEL_ZIP, __VA_ARGS__)
#define DEBUG_PURGE_PERIOD(...) DEBUG_PRINT(LOG_LEVEL_PURGE_PERIOD, __VA_ARGS__)
#define DEBUG_PURGE(...)        DEBUG_PRINT(LOG_LEVEL_PURGE, __VA_ARGS__)

#define COUNT_OF(x)     (sizeof(x) / sizeof(x[0]))

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

//#define db_collect_history  "SELECT ROUND(AVG(value),2) FROM history WHERE detail_level=0 AND topic_id=? AND timestamp>=? GROUP BY ROUND(timestamp/?) ORDER BY timestamp DESC"
#define db_collect_history  "SELECT ROUND(MAX(timestamp/?)), COUNT(timestamp), ROUND(AVG(value),2) FROM history WHERE topic_id=? AND timestamp>=? GROUP BY ROUND(timestamp/?) ORDER BY timestamp DESC"
#define db_last_history     "SELECT ROUND(value,2) FROM history WHERE detail_level=0 AND topic_id=? ORDER BY timestamp DESC LIMIT 1"
//#define db_purge_history    "SELECT MAX(id),MAX(detail_level),MAX(timestamp),topic_id,ROUND(AVG(value),2) FROM `history` WHERE timestamp<? GROUP BY ROUND(timestamp/?),topic_id"
#define db_purge_history    "INSERT INTO `purge_temp` (id,detail_level,timestamp,topic_id,value) SELECT MAX(id),MAX(detail_level),MAX(timestamp),topic_id,ROUND(AVG(value),2) FROM `history` WHERE timestamp<? GROUP BY ROUND(timestamp/?),topic_id"



#define mqtt_host "localhost"
#define mqtt_port 1883

#ifndef STRING_SIZE
#define STRING_SIZE     2048
#endif

#define JSON_BUFFER_SIZE (0x10000 * 4)
#define ZIP_BUFFER_SIZE  (0x10000 * 2)

static int run = 1;
static MYSQL *connection = NULL;
static MYSQL_STMT *history_collect_stmt = NULL;
static MYSQL_STMT *history_last_stmt = NULL;
static MYSQL_STMT *history_purge_stmt = NULL;
static unsigned int last_update_time = 0;
static unsigned int last_purge_time = 0;

enum
{
    PERIOD_TYPE_LIVE    = 0,
    PERIOD_TYPE_ACCUM   = 1,
    PERIOD_TYPE_1HOUR   = 2,
    PERIOD_TYPE_4HOUR   = 3,
    PERIOD_TYPE_1DAY    = 4,
    PERIOD_TYPE_1WEEK   = 5,
    PERIOD_TYPE_1MONTH  = 6,
    PERIOD_TYPE_MAX,
};
#define SECOND      1
#define MINUTE      (60*SECOND)
#define HOUR        (60*MINUTE)
#define DAY         (24*HOUR)
#define WEEK        (7*DAY)
#define MONTH       (32*DAY)
#define YEAR        (366*DAY)
#if 1
const int period_interval[] = {SECOND, 0, MINUTE * 1, MINUTE * 5, HOUR / 2, HOUR * 3, HOUR * 12};
const int period_grid[] = {1, 0, 1*MINUTE, 30*MINUTE, HOUR, 12*HOUR, DAY};
const int period_count[] = {60, 0, 60, 48, 48, 56, 64};
#else
const int period_interval[] = {SECOND, -1, MINUTE * 2, MINUTE * 10, HOUR / 2, HOUR * 6, HOUR * 24};
const int period_grid[] = {1, 0, 1*MINUTE, 30*MINUTE, HOUR, DAY, DAY};
const int period_count[] = {60, -1, 30, 24, 24 * 2, 28, 32};
#endif

static const int purge_time_period[] = { DAY, WEEK, MONTH, YEAR, };
static const int purge_time_interval[] = { MINUTE, HOUR, 6 * HOUR, DAY, };

#define DEFAULT_UPDATE_INTEVAL  (MINUTE * 1000)
#define DEFAULT_PURGE_INTEVAL   (DAY * 1000)


inline unsigned int get_time_ms()
{
    struct timespec cur;
    clock_gettime(CLOCK_MONOTONIC, &cur);
    return cur.tv_sec * 1000 + cur.tv_nsec / 1000000;
}

void print_dump(void *buf, int size, int alignment, FILE *fp)
{
    unsigned char *p = (unsigned char *)buf;
    char s[1024];
    int n = 0;
    
    if (buf == NULL || fp == NULL)
        return;
    
    fprintf(fp, "%5.5X:\t", 0);
    while(size--)
    {
        fprintf(fp, "%2.2X ", *p);
        if ((*p>0x1f) && (*p<0x80))
            s[n++] = *p;
        else
            s[n++] = '.';
        p++;
        if (n % alignment == 0)
        {
            s[n] = 0;
            fprintf(fp, "   \"%s\"\n", s);
            n = 0;
            if (size)
            {
                fprintf(fp, "%5.5X:\t", (int)p - (int)buf);
            }
        }
    }
    if (n)
    {
        s[n] = 0;
        fprintf(fp, "%*s\"%s\"\n", (alignment-n)*3+3, " ", s);
    }
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
    
    DEBUG_TOPICS("------------------------------------\n");
    while ((row = mysql_fetch_row(result))) 
    {
        topics = (topic_t *)realloc(topics, sizeof(topic_t) * (num_topics + 1));
        topics[num_topics].id = atoi(row[0]);
        topics[num_topics].topic = strdup(row[1]);
        DEBUG_TOPICS("[%d] %s\n", topics[num_topics].id, topics[num_topics].topic);
        num_topics ++;
    }
    
    mysql_free_result(result);
    
    DEBUG_TOPICS("------------------------------------\n");
    return 0;
}

int compress_zip(const char *name, void *in, int in_size, void *out, int out_size, int level)
{
    zipFile zf;
    zip_fileinfo zi;
    FILE *f;
    int size = 0;
    
    zf = zipOpen("/tmp/history.zip", APPEND_STATUS_CREATE);
    
    zi.tmz_date.tm_sec = zi.tmz_date.tm_min = zi.tmz_date.tm_hour =
        zi.tmz_date.tm_mday = zi.tmz_date.tm_mon = zi.tmz_date.tm_year = 0;
    zi.dosDate = 0;
    zi.internal_fa = 0;
    zi.external_fa = 0;
    zipOpenNewFileInZip(zf, name, &zi,
        NULL,0,NULL,0,NULL /* comment*/,
        Z_DEFLATED, Z_DEFAULT_COMPRESSION);
    zipWriteInFileInZip(zf, in, in_size);
    zipCloseFileInZip(zf);
    zipClose(zf,NULL);
    
    f = fopen("/tmp/history.zip", "rb");
    if (f)
    {
        size = fread(out, 1, out_size, f);
        fclose(f);
    }
    
    return size;
}

int do_collect(char *out, int type, int topic_idx)
{
    MYSQL_BIND bind[4];
    MYSQL_BIND result_bind[3];
    char str[3][STRING_SIZE];
    my_bool is_null[3], error[3];
    unsigned long length[3];
    float last_value = FLT_MIN;
    float *values;
    int i, c;
    int topic_id;
    
    int start_time, now;
    MYSQL_RES *result;
    char *p = out;
    int interval, grid, count;
    
    interval = period_interval[type];
    grid = period_grid[type];
    count = period_count[type];
    topic_id = topics[topic_idx].id;
    
    now = time(NULL);
    //DEBUG_COLLECT("Now = %d\n", now);
    now = (now + grid - 1) / grid * grid;
    //DEBUG_COLLECT("Grid now = %d\n", now);
    start_time = now - interval * count;
    //DEBUG_COLLECT("Start = %d\n", start_time);
    
    DEBUG_COLLECT("======  Type=%d Topic=%d[%s] I=%d G=%d C=%d [%d-%d] %d  ======\n", type, topic_id, topics[topic_idx].topic, interval, grid, count, start_time, now, now - start_time);
    
    if (type == PERIOD_TYPE_LIVE)
    {
        // ************ Get last value *************
        memset(bind, 0, sizeof(bind));
        bind[0].buffer_type = MYSQL_TYPE_LONG;
        bind[0].buffer = &topic_id;
        bind[0].buffer_length= 0;
        bind[0].is_null= 0;
        
        mysql_stmt_bind_param(history_last_stmt, bind);
        if (!mysql_stmt_execute(history_last_stmt))
        {
            result = mysql_stmt_result_metadata(history_collect_stmt);
            if (result) 
            {
                memset(result_bind, 0, sizeof(result_bind));
                result_bind[0].buffer_type= MYSQL_TYPE_STRING;
                result_bind[0].buffer= (char *)str[0];
                result_bind[0].buffer_length= STRING_SIZE;
                result_bind[0].is_null= &is_null[0];
                result_bind[0].length= &length[0];
                result_bind[0].error= &error[0];
                
                mysql_stmt_bind_result(history_collect_stmt, result_bind);
                mysql_stmt_store_result(history_collect_stmt);
                if (!mysql_stmt_fetch(history_collect_stmt))
                {
                    if (!is_null[0])
                    {
                        last_value = atof(str[0]);
                    }
                }
            }
        }
    }
    
    // ************ Collect History *************
    memset(bind, 0, sizeof(bind));
    bind[0].buffer_type = MYSQL_TYPE_LONG;
    bind[0].buffer = &interval;
    bind[0].buffer_length= 0;
    bind[0].is_null= 0;
    bind[1].buffer_type = MYSQL_TYPE_LONG;
    bind[1].buffer = &topic_id;
    bind[1].buffer_length= 0;
    bind[1].is_null= 0;
    bind[2].buffer_type = MYSQL_TYPE_LONG;
    bind[2].buffer = &start_time;
    bind[2].buffer_length= 0;
    bind[2].is_null= 0;
    bind[3].buffer_type = MYSQL_TYPE_LONG;
    bind[3].buffer = &interval;
    bind[3].buffer_length= 0;
    bind[3].is_null= 0;
    
    mysql_stmt_bind_param(history_collect_stmt, bind);
    if (mysql_stmt_execute(history_collect_stmt))
    {
        DEBUG_MYSQL("%s\n", mysql_error(connection));
        return p - out;
    }
    
    result = mysql_stmt_result_metadata(history_collect_stmt);
    if (result == NULL) 
    {
        DEBUG_COLLECT("No result\n");
        return p - out;
    }
    
    memset(result_bind, 0, sizeof(result_bind));
    result_bind[0].buffer_type= MYSQL_TYPE_STRING;
    result_bind[0].buffer= (char *)str[0];
    result_bind[0].buffer_length= STRING_SIZE;
    result_bind[0].is_null= &is_null[0];
    result_bind[0].length= &length[0];
    result_bind[0].error= &error[0];
    result_bind[1].buffer_type= MYSQL_TYPE_STRING;
    result_bind[1].buffer= (char *)str[1];
    result_bind[1].buffer_length= STRING_SIZE;
    result_bind[1].is_null= &is_null[1];
    result_bind[1].length= &length[1];
    result_bind[1].error= &error[1];
    result_bind[2].buffer_type= MYSQL_TYPE_STRING;
    result_bind[2].buffer= (char *)str[2];
    result_bind[2].buffer_length= STRING_SIZE;
    result_bind[2].is_null= &is_null[2];
    result_bind[2].length= &length[2];
    result_bind[2].error= &error[2];
    mysql_stmt_bind_result(history_collect_stmt, result_bind);
    mysql_stmt_store_result(history_collect_stmt);
    values = (float *)malloc(sizeof(float) * (count + 1));
    for (i = 0; i < count; i ++)
    {
        values[i] = FLT_MIN;
    }

    now = now / interval * interval;
    p += sprintf(p, "{\\\\\\\"period_type\\\\\\\":%d,\\\\\\\"actual_timestamp\\\\\\\":%lld,\\\\\\\"aggregation_period\\\\\\\":%lld,\\\\\\\"dots\\\\\\\":[", type, (long long)now * 1000, (long long)interval * 1000);
    while (!mysql_stmt_fetch(history_collect_stmt)) 
    {
        if (!is_null[2])
        {
            c = now / interval - atol(str[0]);
            values[c] = atof(str[2]);
        }
    }
    
    if (type == PERIOD_TYPE_LIVE)
    {
        if (last_value != FLT_MIN)
        {
            for (i = 0; i < count; i ++)
            {
                if (values[i] == FLT_MIN)
                {
                    values[i] = last_value;
                }
                else
                {
                    break;
                }
            }
            last_value = FLT_MIN;
            for (i = count - 1; i >= 0; i--)
            {
                if (values[i] == FLT_MIN)
                {
                    values[i] = last_value;
                }
                else
                {
                    last_value = values[i];
                }
            }
        }
    }
    else
    {
        for (i = 0; i < count; i ++)
        {
            if (values[i] != FLT_MIN)
            {
                break;
            }
        }
        c = i;
        last_value = FLT_MIN;
        for (i = count - 1; i >= c; i--)
        {
            if (values[i] == FLT_MIN)
            {
                values[i] = last_value;
            }
            else
            {
                last_value = values[i];
            }
        }
    }
    
    for (i = 0; i < count; i ++)
    {
        if (i == 0)
        {
            if (values[i] == FLT_MIN)
            {
                p += sprintf(p, "\\\\\\\"\\\\\\\"");
            }
            else
            {
                p += sprintf(p, "%.2f", values[i]);
            }
        }
        else
        {
            if (values[i] == FLT_MIN)
            {
                p += sprintf(p, ",\\\\\\\"\\\\\\\"");
            }
            else
            {
                p += sprintf(p, ",%.2f", values[i]);
            }
        }
    }
    p += sprintf(p, "]},");
    mysql_free_result(result);
    free(values);
    
    DEBUG_COLLECT("Out=%s\n", out);
    
    return p - out;
}

int do_update(struct mosquitto *mosq)
{
    int i, j;
    char *temp, *p;
    char *utf16, *pp;
    char *zipped;
    int zip_size;
    
    DEBUG_UPDATE("DO UPDATE\n");
    temp = (char *)malloc(JSON_BUFFER_SIZE);
    p = temp;
    
    p += sprintf(p, "{\"ver\":1,\"type\":\"topics_data\",\"data\":\"[");
    for (i = 0; i < num_topics; i ++)
    {
        if (strstr(topics[i].topic, "temperature")
            || strstr(topics[i].topic, "pressure")
            || strstr(topics[i].topic, "battery")
            || strstr(topics[i].topic, "distance") )
        {
            p += sprintf(p, "{\\\"topic\\\":\\\"%s_$hd\\\",\\\"payload\\\":\\\"{\\\\\\\"type\\\\\\\":\\\\\\\"graph_history\\\\\\\",\\\\\\\"graphics\\\\\\\":[", topics[i].topic);
            for (j = PERIOD_TYPE_1HOUR; j < PERIOD_TYPE_MAX; j ++)
            {
                p += do_collect(p, j, i);
            }
            if (*(p-1) == ',')
            {
                p --;
                *p = 0;
            }
            p += sprintf(p, "]}\\\"},");
        }
    }
    if (*(p-1) == ',')
    {
        p --;
        *p = 0;
    }
    p += sprintf(p, "]\"}");
    
    utf16 = (char *)malloc(JSON_BUFFER_SIZE * 2);
    pp = utf16;
    p = temp;
    while (*p != 0)
    {
        *pp++ = 0;
        *pp++ = *p++;
    }
    
    DEBUG_JSON("JSON=\n%s\n", temp);
    
    zipped = (char *)malloc(ZIP_BUFFER_SIZE);
    memset(zipped, 0, ZIP_BUFFER_SIZE);
    
    zip_size = compress_zip("data", utf16, pp - utf16, zipped, ZIP_BUFFER_SIZE, Z_DEFAULT_COMPRESSION);
    
    DEBUG_ZIP("Zipped size %d\n", zip_size);
    //print_dump(zipped, zip_size, 32, stdout);
    
    int mid_sent = 0;
    int rc = MOSQ_ERR_SUCCESS;
    rc = mosquitto_publish(mosq, &mid_sent, "/home/history2", zip_size, zipped, 1, true);
    if(rc){
        switch(rc){
        case MOSQ_ERR_INVAL:
            DEBUG_MQTT("Error: Invalid input. Does your topic contain '+' or '#'?\n");
            break;
        case MOSQ_ERR_NOMEM:
            DEBUG_MQTT("Error: Out of memory when trying to publish message.\n");
            break;
        case MOSQ_ERR_NO_CONN:
            DEBUG_MQTT("Error: Client not connected when trying to publish.\n");
            break;
        case MOSQ_ERR_PROTOCOL:
            DEBUG_MQTT("Error: Protocol error when communicating with broker.\n");
            break;
        case MOSQ_ERR_PAYLOAD_SIZE:
            DEBUG_MQTT("Error: Message payload is too large.\n");
            break;
        }
    }
    
    free(temp);
    
    return 0;
}

int do_purge_period(int end_time, int interval)
{
    char sql_temp[1024];

    DEBUG_PURGE_PERIOD("  Purge period from %d for interval %d\n", end_time, interval);

    sprintf(sql_temp, "TRUNCATE TABLE `purge_temp`");
    DEBUG_PURGE_PERIOD("  RUNNING [%s]\n", sql_temp);
    if (mysql_query(connection, sql_temp)) 
    {
        DEBUG_MYSQL("%s\n", mysql_error(connection));
        return 1;
    }
    DEBUG_PURGE_PERIOD("    Got %lld rows\n", mysql_affected_rows(connection));

    sprintf(sql_temp, "INSERT INTO `purge_temp` (id,detail_level,timestamp,topic_id,value) SELECT MAX(id),MAX(detail_level),MAX(timestamp),topic_id,ROUND(AVG(value),2) FROM `history` WHERE timestamp<%d GROUP BY ROUND(timestamp/%d),topic_id",
        end_time, interval);
    DEBUG_PURGE_PERIOD("  RUNNING [%s]\n", sql_temp);
    if (mysql_query(connection, sql_temp)) 
    {
        DEBUG_MYSQL("%s\n", mysql_error(connection));
        return 1;
    }
    DEBUG_PURGE_PERIOD("    Got %lld rows\n", mysql_affected_rows(connection));

    sprintf(sql_temp, "DELETE FROM `history` WHERE timestamp<%d", end_time);
    DEBUG_PURGE_PERIOD("  RUNNING [%s]\n", sql_temp);
    if (mysql_query(connection, sql_temp)) 
    {
        DEBUG_MYSQL("%s\n", mysql_error(connection));
    }
    DEBUG_PURGE_PERIOD("    Got %lld rows\n", mysql_affected_rows(connection));

    sprintf(sql_temp, "INSERT INTO `history` SELECT * FROM `purge_temp`");
    DEBUG_PURGE_PERIOD("  RUNNING [%s]\n", sql_temp);
    if (mysql_query(connection, sql_temp)) 
    {
        DEBUG_MYSQL("%s\n", mysql_error(connection));
    }
    DEBUG_PURGE_PERIOD("    Got %lld rows\n", mysql_affected_rows(connection));

    DEBUG_PURGE_PERIOD("  Purge period done\n\n");

    return 0;
}

int do_purge(void)
{
    int now, end_time, interval;
    int i;

    now = time(NULL);
    DEBUG_PURGE("DO PURGE time=%d\n", now);

    for (i = 0; i < COUNT_OF(purge_time_period); i ++)
    {
        end_time = (now - purge_time_period[i]);
        interval = purge_time_interval[i];
        end_time = (end_time / interval) * interval;
        DEBUG_PURGE("Purge from %d for interval %d\n", end_time, interval);
        do_purge_period(end_time, interval);
    }
    DEBUG_PURGE("PURGE DONE\n");

    return 0;
}

int main(int argc, char **argv)
{
    my_bool reconnect = true;
    char clientid[24];
    struct mosquitto *mosq;
    int rc = 0;
    unsigned int now;
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
        
        history_collect_stmt = mysql_stmt_init(connection);
        history_last_stmt = mysql_stmt_init(connection);
        history_purge_stmt = mysql_stmt_init(connection);
        mysql_stmt_prepare(history_collect_stmt, db_collect_history, strlen(db_collect_history));
        mysql_stmt_prepare(history_purge_stmt, db_purge_history, strlen(db_purge_history));
        
        memset(clientid, 0, 24);
        snprintf(clientid, 23, "mqtt_update_%d", getpid());
        mosq = mosquitto_new(clientid, true, connection);
        
        if(mosq)
        {
            mosquitto_connect_callback_set(mosq, connect_callback);
            
            rc = mosquitto_connect(mosq, mqtt_host, mqtt_port, 60);
            
            last_update_time = 0;//get_time_ms();
            last_purge_time = get_time_ms() - DEFAULT_PURGE_INTEVAL;
            
            while(run)
            {
                rc = mosquitto_loop(mosq, -1, 1);
                if(run && rc)
                {
                    sleep(20);
                    mosquitto_reconnect(mosq);
                }
                
                now = get_time_ms();
                if ((now - last_update_time) >= DEFAULT_UPDATE_INTEVAL)
                {
                    last_update_time = now;
                    do_update(mosq);
                }
                if ((now - last_purge_time) >= DEFAULT_PURGE_INTEVAL)
                {
                    last_purge_time = now;
                    do_purge();
                }
            }
            mosquitto_destroy(mosq);
        }
        
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
