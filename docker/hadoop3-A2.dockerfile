FROM ubuntu:22.04

ENV HADOOP_HOME /opt/hadoop
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

RUN apt update && apt install -y ssh rsync vim openjdk-8-jdk openssh-server openssh-client wget htop python3-pip wormhole
RUN pip install flask requests gdown

RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-3.2.2/hadoop-3.2.2.tar.gz \
    && tar -xzf hadoop-3.2.2.tar.gz \
    && mv hadoop-3.2.2 ${HADOOP_HOME} \
    && echo "export JAVA_HOME=$JAVA_HOME" >> ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh \
    && echo "PATH=$PATH:$HADOOP_HOME/bin" >> ~/.bashrc

RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa \
    && cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys \
    && chmod 0600 ~/.ssh/authorized_keys

RUN mkdir /home/root
RUN mkdir /home/root/tmpdata
RUN mkdir /home/root/dfsdata  \
    && mkdir /home/root/dfsdata/namenode  \
    && mkdir /home/root/dfsdata/datanode

RUN chown -R root:root /home/root/dfsdata/ \
    && chown -R root:root /home/root/dfsdata/namenode \
    && chown -R root:root /home/root/dfsdata/datanode

ENV HDFS_NAMENODE_USER="root"
ENV HDFS_DATANODE_USER="root"
ENV HDFS_SECONDARYNAMENODE_USER="root"
ENV YARN_RESOURCEMANAGER_USER="root"
ENV YARN_NODEMANAGER_USER="root"

ADD hadoop_config/*xml ${HADOOP_HOME}/etc/hadoop/
ADD hadoop_config/ssh_config /root/.ssh/config
ADD hadoop_config/start-hadoop.sh start-hadoop.sh
ADD hadoop_config/restart-hadoop.sh restart-hadoop.sh
ADD hadoop_server-A2.py hadoop_server.py

RUN chown root:root /root/.ssh/config
RUN chmod 644 /root/.ssh/config

RUN chmod +x hadoop_server.py start-hadoop.sh restart-hadoop.sh
RUN rm -rf ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh
ADD hadoop_config/hadoop-env.sh ${HADOOP_HOME}/etc/hadoop/
RUN gdown --id 1tre0xmwwUibHBXrn1n9iEhcWIhsreTep
RUN mkdir A1 && mv /dataset.json /A1/

# add new ports here
EXPOSE 8088 50070 50075 50030 50060 9870 10000 19888
# RUN swapoff -a
CMD ["python3", "-u", "hadoop_server.py"]