#!/bin/bash

# Start SSH daemon
echo "[entrypoint] Starting SSH daemon..."
service ssh start || true

# Set PDSH to use SSH
echo "ssh" > /etc/pdsh/rcmd_default

# Get node type from environment variable
NODE_TYPE=${NODE_TYPE:-namenode}
echo "[entrypoint] NODE_TYPE=$NODE_TYPE"

case "$NODE_TYPE" in
  namenode)
    echo "==> Starting NameNode"
    if [ ! -d "/hadoop/dfs/name/current" ]; then
      echo "==> Formatting HDFS NameNode..."
      $HADOOP_HOME/bin/hdfs namenode -format -force
    fi
    $HADOOP_HOME/bin/hdfs namenode &
    echo "==> Waiting 30s for NameNode to be ready..."
    sleep 30
    echo "==> Starting YARN ResourceManager..."
    $HADOOP_HOME/bin/yarn resourcemanager &
    echo "==> Starting MapReduce History Server..."
    $HADOOP_HOME/bin/mapred historyserver &
    ;;
  secondarynamenode)
    echo "==> Starting Secondary NameNode"
    sleep 15
    $HADOOP_HOME/bin/hdfs secondarynamenode &
    ;;
  datanode)
    echo "==> Starting DataNode"
    sleep 20
    $HADOOP_HOME/bin/hdfs datanode &
    $HADOOP_HOME/bin/yarn nodemanager &
    ;;
  *)
    echo "Unknown NODE_TYPE: $NODE_TYPE"
    exit 1
    ;;
esac

# Keep container alive
tail -f /dev/null
