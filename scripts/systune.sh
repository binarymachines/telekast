#!/bin/bash

# system tuning for Kafka brokers

# network settings

echo 'net.core.wmem_max=16777216' >> /etc/sysctl.conf
echo 'net.core.rmem_max=16777216' >> /etc/sysctl.conf
echo 'net.core.somaxconn = 1000' >> /etc/sysctl.conf
echo 'net.core.netdev_max_backlog = 5000' >> /etc/sysctl.conf
echo 'net.ipv4.tcp_wmem = 4096 12582912 16777216' >> /etc/sysctl.conf
echo 'net.ipv4.tcp_rmem = 4096 12582912 16777216' >> /etc/sysctl.conf


# virtual memory
echo 'vm.swappiness = 1' >> /etc/sysctl.conf
echo 'vm.dirty_ratio = 90' >> /etc/sysctl.conf
echo 'vm.dirty_background_ratio = 80' >> /etc/sysctl.conf
