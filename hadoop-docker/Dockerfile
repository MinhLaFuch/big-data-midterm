FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies
RUN apt-get update && apt-get install -y \
    openjdk-8-jdk \
    ssh \
    pdsh \
    wget \
    curl \
    net-tools \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Download and install Hadoop 3.2.1
RUN wget -q https://archive.apache.org/dist/hadoop/common/hadoop-3.2.1/hadoop-3.2.1.tar.gz -P /tmp && \
    tar -xzf /tmp/hadoop-3.2.1.tar.gz -C /opt/ && \
    rm /tmp/hadoop-3.2.1.tar.gz && \
    mv /opt/hadoop-3.2.1 /opt/hadoop

ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV HDFS_SECONDARYNAMENODE_USER=root
ENV YARN_RESOURCEMANAGER_USER=root
ENV YARN_NODEMANAGER_USER=root

# Configure Hadoop env
RUN echo "export JAVA_HOME=${JAVA_HOME}" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh && \
    echo "export HADOOP_HOME=${HADOOP_HOME}" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh && \
    echo "export HDFS_NAMENODE_USER=root" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh && \
    echo "export HDFS_DATANODE_USER=root" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh && \
    echo "export HDFS_SECONDARYNAMENODE_USER=root" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh && \
    echo "export YARN_RESOURCEMANAGER_USER=root" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh && \
    echo "export YARN_NODEMANAGER_USER=root" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh

# Setup SSH (passwordless)
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys && \
    echo "ssh" > /etc/pdsh/rcmd_default

RUN echo "StrictHostKeyChecking no" >> /etc/ssh/ssh_config

# Copy Hadoop XML configs
COPY config/ $HADOOP_HOME/etc/hadoop/

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 9870 9000 8088 8042 9864 19888

ENTRYPOINT ["/entrypoint.sh"]
