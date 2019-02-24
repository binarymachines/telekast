#!/bin/bash

USAGE_STRING="Usage: zksetup -p <pkg_manager>"

if [ "$#" -ne 2 ]
then
    echo $USAGE_STRING
    exit 1
fi

PKG_MGR=$2

sudo $PKG_MGR update
sudo $PKG_MGR install -y git
sudo $PKG_MGR install -y telnet
sudo $PKG_MGR install -y java-1.8.0-openjdk
mkdir installs
cd installs/
wget https://www-us.apache.org/dist/zookeeper/stable/zookeeper-3.4.12.tar.gz
tar -xvzf zookeeper-3.4.12.tar.gz 
sudo mv zookeeper-3.4.12 /opt/
sudo ln -s /opt/zookeeper-3.4.12/ /opt/zookeeper
sudo mkdir /var/lib/zookeeper
sudo mkdir /var/log/zookeeper
cd /opt/zookeeper/conf
sudo cp zoo_sample.cfg zoo.cfg

sudo useradd zkuser
sudo /usr/sbin/usermod -d /opt/zookeeper-3.4.12 zkuser

sudo chown -R zkuser:zkuser /opt/zookeeper-3.4.12/ 
sudo chown -R zkuser:zkuser /var/lib/zookeeper
sudo chown -R zkuser:zkuser /var/log/zookeeper