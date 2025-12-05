#!/bin/bash
# Assumes Kafka is installed in ~/kafka
KAFKA_HOME=~/kafka

if [ ! -d "$KAFKA_HOME" ]; then
    echo "Kafka directory not found at $KAFKA_HOME. Please update script."
    exit 1
fi

echo "Starting Zookeeper..."
$KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties

echo "Starting Kafka Broker..."
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties

echo "Kafka services started."

