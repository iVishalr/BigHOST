#!/bin/sh
CONVERGE=1
ITER=1
rm v v1 log*

$HADOOP_HOME/bin/hadoop dfsadmin -safemode leave
hdfs dfs -rm -r /test_page_rank/output 

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.2.2.jar \
-mapper "'/Assign2/m.py'" \
-reducer "'/Assign2/r.py' '/Assign2/v'" \
-input /test_page_rank/input/dataset_1percent.txt \
-output /test_page_rank/output/task-1-output

while [ "$CONVERGE" -ne 0 ]
do
	echo "############################# ITERATION $ITER #############################"
	hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.2.2.jar \
	-mapper "'/Assign2/m2.py' '/Assign2/v' '/Assign2/datasets/embedding_1percent.json'" \
	-reducer "'/Assign2/r2.py'" \
	-input /test_page_rank/output/task-1-output/part-00000 \
	-output /test_page_rank/output/task-2-output/
	touch v1
	hadoop dfs -cat /test_page_rank/output/task-2-output/part-00000 > "/Assign2/v1"
	CONVERGE=$(python3 check_conv.py $ITER>&1)
	# CONVERGE=0
	ITER=$((ITER+1))
	hdfs dfs -rm -r /test_page_rank/output/task-2-output/
	echo $CONVERGE
done
