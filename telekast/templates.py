#!/usr/bin/env python


ZOO_CFG_TEMPLATE = '''
# The number of milliseconds of each tick
tickTime=2000
# The number of ticks that the initial 
# synchronization phase can take
initLimit=10
# The number of ticks that can pass between 
# sending a request and getting an acknowledgement
syncLimit=2
# the directory where the snapshot is stored.
# do not use /tmp for storage, /tmp here is just 
# example sakes.
dataDir={{zookeeper_data_dir}}
dataLogDir={{zookeeper_log_dir}}
# the port at which the clients will connect
clientPort=2181
# the maximum number of client connections.
# increase this if you need to handle more clients
maxClientCnxns=0
#
# Be sure to read the maintenance section of the 
# administrator guide before turning on autopurge.
#
# http://zookeeper.apache.org/doc/current/zookeeperAdmin.html#sc_maintenance
#
# The number of snapshots to retain in dataDir
#autopurge.snapRetainCount=3
# Purge task interval in hours
# Set to "0" to disable auto purge feature
#autopurge.purgeInterval=1
{% for zookeeper_server in cluster %}
server.{{zookeeper_server.index}}={{zookeeper_server.hostname}}:2888:3888
{% endfor %}
'''


ZOOKEEPER_SYSCTL_TEMPLATE = '''
[Unit]
Description=Zookeeper Daemon
Documentation=http://zookeeper.apache.org
Requires=network.target
After=network.target

[Service]    
Type=forking
WorkingDirectory={{zookeeper_install_dir}}
User={{zookeeper_user}}
Group={{zookeeper_group}}
ExecStart={{zookeeper_install_dir}}/bin/zkServer.sh start {{zookeeper_install_dir}}/conf/zoo.cfg
ExecStop={{zookeeper_install_dir}}/bin/zkServer.sh stop {{zookeeper_install_dir}}/conf/zoo.cfg
ExecReload={{zookeeper_install_dir}}/bin/zkServer.sh restart {{zookeeper_install_dir}}/conf/zoo.cfg
TimeoutSec=30
Restart=on-failure

[Install]
WantedBy=default.target
'''