FROM ubuntu:22.04

RUN apt update -y
RUN apt install openssh-server openssh-client openjdk-8-jdk  -y 

USER root
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa  \
    && cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys \
    && chmod 0600 ~/.ssh/authorized_keys

# RUN wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.3/hadoop-3.3.3.tar.gz -o /home/root/hadoop-3.3.3.tar.gz \
#     && tar -xzf /home/root/hadoop-3.3.3.tar.gz -C /home/root/ \
#     && rm /home/root/hadoop-3.3.3.tar.gz

COPY docker/hadoop-3.3.3.tar.gz /home/root/hadoop-3.3.3.tar.gz
RUN tar -xzf /home/root/hadoop-3.3.3.tar.gz -C /home/root/  \
    && rm /home/root/hadoop-3.3.3.tar.gz

RUN mkdir /home/root/tmpdata
RUN mkdir /home/root/dfsdata  \
    && mkdir /home/root/dfsdata/namenode  \
    && mkdir /home/root/dfsdata/datanode

RUN chown -R root:root /home/root/dfsdata/ \
    && chown -R root:root /home/root/dfsdata/namenode \
    && chown -R root:root /home/root/dfsdata/datanode

ENV HADOOP_HOME=/home/root/hadoop-3.3.3
ENV HADOOP_INSTALL=$HADOOP_HOME
ENV HADOOP_MAPRED_HOME=$HADOOP_HOME
ENV HADOOP_COMMON_HOME=$HADOOP_HOME
ENV HADOOP_HDFS_HOME=$HADOOP_HOME
ENV HADOOP_YARN_HOME=$HADOOP_HOME
ENV HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
ENV HADOOP_OPTS=-Djava.library.path=$HADOOP_HOME/lib/native
ENV HDFS_NAMENODE_USER="root"
ENV HDFS_DATANODE_USER="root"
ENV HDFS_SECONDARYNAMENODE_USER="root"
ENV YARN_RESOURCEMANAGER_USER="root"
ENV YARN_NODEMANAGER_USER="root"

COPY docker/hadoop_config/* /home/root/hadoop-3.3.3/etc/hadoop/
RUN $HADOOP_HOME/bin/hdfs namenode -format
WORKDIR $HADOOP_HOME/sbin/