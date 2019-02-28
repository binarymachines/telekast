#!/bin/bash

#wget https://downloads.lightbend.com/scala/2.12.2/scala-2.12.2.rpm
#sudo yum localinstall -y scala-2.12.2.rpm

sudo apt-get install -y apt-transport-https
sudo apt-get install -y software-properties-common
sudo apt-get install -y dirmngr --install-recommends
sudo apt-get install -y scala

echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
sudo apt-get update
sudo apt-get install -y sbt

### check scala install ###
scala -version

## Install SBT fro RPM ###
curl https://bintray.com/sbt/rpm/rpm | sudo tee /etc/yum.repos.d/bintray-sbt-rpm.repo
sudo yum install -y sbt

### Install Yahoo's Kafka Manager 
### can also download and unzip https://github.com/yahoo/kafka-manager/archive/master.zip 

git clone https://github.com/yahoo/kafka-manager.git
cd ./kafka-manager
sbt clean dist  # this part is going to take some time as it has to download all the dependencies

mv ./target/universal/kafka-manager-1.3.3.8.zip ~/
unzip kafka-manager-1.3.3.8.zip
rm kafka-manager-1.3.3.8.zip
cd kafka-manager-1.3.3.8

### Start Kafka-Manager ###
#./bin/kafka-manager -Dkafka-manager.zkhosts="$ZKHOSTS"

