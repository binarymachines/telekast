#!/bin/bash

USAGE_STRING="Usage: install_librdkafka -p <pkg_manager>"

if [ "$#" -ne 2 ]
then
    echo $USAGE_STRING
    exit 1
fi

PKG_MGR=$2

sudo $PKG_MGR install -y build-essential &&
$PKG_MGR install -y zlib &&
$PKG_MGR install -y libssl-dev &&
$PKG_MGR install -y libsasl2-dev

git clone https://github.com/edenhill/librdkafka.git
cd librdkafka 
./configure
make
sudo make install

