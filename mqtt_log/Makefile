CFLAGS=-Wall -ggdb `mysql_config --cflags`
LDFLAGS=-g ../mosquitto-1.4.11/lib/libmosquitto.so.1 -lminizip -lmysqlclient `mysql_config --cflags --libs`

.PHONY: all clean

all : mqtt_log

mqtt_log : main.o
	${CC} $^ -o $@ ${LDFLAGS}

main.o : main.c
	${CC} -c $^ -o $@ ${CFLAGS} -I../mosquitto-1.4.11/lib

clean : 
	-rm -f *.o mqtt_log
