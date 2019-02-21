#!/bin/bash

USAGE_STRING="showtopic <topic_name>"

if [ $# -ne 1 ]
then
	echo $USAGE_STRING
	exit 1
fi

TOPIC_NAME=$1

{{kafka_install_dir}}/bin/kafka-topics.sh --describe --zookeeper {{zk_host}}:{{zk_port}} --topic $TOPIC_NAME
