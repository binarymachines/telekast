
#!/bin/bash
#
# Installation script for Oracle Java 8 on Ubuntu 
#

sudo add-apt-repository -y ppa:webupd8team/java
sudo debconf /bin/sh -c '. /usr/share/debconf/confmodule; db_set shared/accepted-oracle-license-v1-1 true'
sudo DEBIAN_FRONTEND=noninteractive apt-get -y install oracle-java8-installer


