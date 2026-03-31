#!/bin/bash

# Start SSH daemon
service ssh start

# Set PDSH to use SSH
echo "ssh" > /etc/pdsh/rcmd_default

NODE_TYPE=${NODE_TYPE:-namenode}

case "$NODE_TYPE" in
  namenode)
    echo "==> Starting NameNode"
    # Format HDFS only if not already formatted
    if [ ! -d "/hadoop/dfs/name/current" ]; then
      echo "==> Formatting HDFS NameNode..."
      $HADOOP_HOME/bin/hdfs namenode -format -force
    fi
    $HADOOP_HOME/bin/hdfs namenode &
    $HADOOP_HOME/bin/yarn resourcemanager &
    $HADOOP_HOME/bin/mapred historyserver &
    ;;
  secondarynamenode)
    echo "==> Starting Secondary NameNode"
    sleep 10  # wait for primary namenode
    $HADOOP_HOME/bin/hdfs secondarynamenode &
    ;;
  datanode)
    echo "==> Starting DataNode"
    sleep 15  # wait for namenode
    $HADOOP_HOME/bin/hdfs datanode &
    $HADOOP_HOME/bin/yarn nodemanager &
    ;;
esac

# Keep container alive
tail -f /dev/null
