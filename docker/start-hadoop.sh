# # #!/bin/bash
# echo "Starting Hadoop Scripts"
# # start ssh server
# # /etc/init.d/ssh start

# # format namenode
# echo 'Y' | $HADOOP_HOME/bin/hdfs namenode -format

# # start hadoop
# # $HADOOP_HOME/sbin/start-dfs.sh
# # $HADOOP_HOME/sbin/start-yarn.sh
# $HADOOP_HOME/sbin/mr-jobhistory-daemon.sh start historyserver
# $HADOOP_HOME/sbin/start-all.sh

# echo "Done Starting Hadoop Scripts"
# # keep container running
# # tail -f /dev/null