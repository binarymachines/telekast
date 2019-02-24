#!/bin/bash

USAGE_STRING="Usage: ksetup -p <pkg_manager>"

if [ "$#" -ne 2 ]
then
    echo $USAGE_STRING
    exit 1
fi

PKG_MGR=$2

sudo $PKG_MGR update
sudo $PKG_MGR install -y default-jre
mkdir -p ~/Downloads
sudo mkdir -p /opt/kafka 
sudo useradd kafka -d /opt/kafka
sudo adduser kafka sudo
sudo mkdir /var/log/kafka
sudo chown kafka:kafka /var/log/kafka
cd /opt/kafka
wget http://apache.mirrors.lucidnetworks.net/kafka/2.1.0/kafka_2.12-2.1.0.tgz -O ~/Downloads/kafka.tgz
sudo tar -xvzf ~/Downloads/kafka.tgz --strip 1
sudo chown -R kafka:kafka /opt/kafka
