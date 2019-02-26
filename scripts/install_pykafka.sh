#!/bin/bash


sudo yum install python3-devel
git clone git@github.com:Parsely/pykafka.git && cd pykafka
python3 setup.py develop