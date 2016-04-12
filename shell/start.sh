#!/bin/bash
cd `dirname $0`
BIN_DIR=`pwd`
cd ..
DEPLOY_DIR=`pwd`
CONF_DIR=$DEPLOY_DIR/conf
VALS_FILE=$DEPLOY_DIR/vals.json


PIDS=`ps -f | grep java | grep "$CONF_DIR" |awk '{print $2}'`
if [ -n "$PIDS" ]; then
    echo "PID: $PIDS"
    exit 1
fi

LIB_DIR=$DEPLOY_DIR/lib
LIB_JARS=$DEPLOY_DIR/lib/*

JAVA_OPTS=" -Djava.awt.headless=true -Djava.net.preferIPv4Stack=true "

echo -e "Starting the application ...\c"
java $JAVA_OPTS -classpath $DEPLOY_DIR:$LIB_JARS com.ai.opt.tools.kafka.KafkaProduce

