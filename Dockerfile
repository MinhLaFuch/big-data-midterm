FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

# Install Java 8, SSH, pdsh, and dos2unix for line ending conversion
RUN apt-get update && apt-get install -y \
    openjdk-8-jdk \
    ssh \
    pdsh \
    wget \
    dos2unix \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Download and install Hadoop 3.2.1
COPY hadoop-3.2.1.tar.gz /tmp/
RUN tar -xzf /tmp/hadoop-3.2.1.tar.gz -C /opt/ && \
    mv /opt/hadoop-3.2.1 /opt/hadoop && \
    rm /tmp/hadoop-3.2.1.tar.gz

ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

# Setup passwordless SSH
RUN mkdir -p ~/.ssh && \
    ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys && \
    echo "ssh" > /etc/pdsh/rcmd_default && \
    chmod +x /etc/pdsh/rcmd_default

# Copy and convert entrypoint script
COPY entrypoint.sh /tmp/entrypoint.sh
RUN dos2unix /tmp/entrypoint.sh && \
    mv /tmp/entrypoint.sh /entrypoint.sh && \
    chmod +x /entrypoint.sh

# Copy Hadoop configuration files
COPY config/ $HADOOP_HOME/etc/hadoop/

# Create necessary directories
RUN mkdir -p /hadoop/dfs/name && \
    mkdir -p /hadoop/dfs/data

ENTRYPOINT ["/entrypoint.sh"]
