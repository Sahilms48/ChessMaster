#!/bin/bash
KAFKA_HOME=~/kafka

echo "Stopping Kafka Broker..."
$KAFKA_HOME/bin/kafka-server-stop.sh

echo "Stopping Zookeeper..."
$KAFKA_HOME/bin/zookeeper-server-stop.sh

echo "Kafka services stopped."

