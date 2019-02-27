#!/bin/bash


sudo apt-get install -y gpg
sudo apt-get install -y dirmngr
sudo add-apt-repository ppa:webupd8team/java
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys C2518248EEA14886
sudo apt update
sudo apt install -y oracle-java8-installer
sudo update-alternatives --config java




