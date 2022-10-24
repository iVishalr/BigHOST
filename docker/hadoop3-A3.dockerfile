FROM ubuntu:22.04

ENV HADOOP_HOME /opt/hadoop
ENV SPARK_HOME /opt/spark
ENV KAFKA_HOME /usr/local/kafka
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

RUN apt update && apt install -y ssh rsync vim openjdk-8-jdk openssh-server openssh-client wget htop python3-pip wormhole scala
RUN pip install flask requests gdown pyspark==3.2.2 kafka-python==2.0.2

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
ADD hadoop_server-A3.py hadoop_server.py

RUN chown root:root /root/.ssh/config
RUN chmod 644 /root/.ssh/config

RUN chmod +x hadoop_server.py start-hadoop.sh restart-hadoop.sh
RUN rm -rf ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh
ADD hadoop_config/hadoop-env.sh ${HADOOP_HOME}/etc/hadoop/

RUN wget https://downloads.apache.org/kafka/3.2.3/kafka_2.12-3.2.3.tgz \
    && tar -xvzf kafka_2.12-3.2.3.tgz \
    && mv kafka_2.12-3.2.3 ${KAFKA_HOME}

ADD kafka_config/*.service /etc/systemd/system/
RUN chown -R root:root /etc/systemd/system/kafka.service \
    && chown -R root:root /etc/systemd/system/zookeeper.service

RUN wget https://archive.apache.org/dist/spark/spark-3.2.2/spark-3.2.2-bin-hadoop3.2.tgz \
    && tar -xvzf spark-3.2.2-bin-hadoop3.2.tgz \
    && mv spark-3.2.2-bin-hadoop3.2 ${SPARK_HOME} \
    && echo "export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin" >> ~/.bashrc \
    && echo "export PYSPARK_PYTHON=/usr/bin/python3" >> ~/.bashrc

RUN chmod +x ${SPARK_HOME}/sbin/*.sh \
    && chmod +x ${SPARK_HOME}/bin/pyspark \
    && chmod +x ${SPARK_HOME}/bin/spark-shell \
    && chmod +x ${SPARK_HOME}/bin/spark-submit

ADD spark_config/*.properties ${SPARK_HOME}/conf/
ENV HADOOP_CONF_DIR ${HADOOP_HOME}/etc/hadoop
ENV YARN_CONF_DIR ${HADOOP_HOME}/etc/hadoop

RUN gdown --id 1OZHGglsceA8ZGH5uWI9dkLHU2NI4ytQO
RUN gdown --id 1x8I8fneLQhYtR-r0rqjDYQSGMkalip2G
RUN gdown --id 1khV5A0X14q7rHqYZDeoS0nuKI4AhJsWA
RUN gdown --id 1LpDceRLDTvGOY-H1bKJm2X3a9GhB3LzQ
RUN mkdir A2 && mv /web-BerkStan.txt /A2/graph.txt
RUN mv page_embeddings.json /A2/
RUN mv w /A2/
RUN mv adjacency_list.txt /A2/

# add new ports here
EXPOSE 8088 50070 50075 50030 50060 9870 10000 19888
# RUN swapoff -a
CMD ["python3", "-u", "hadoop_server.py"]