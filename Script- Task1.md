docker-compose up -d

mvn clean install

docker cp target/Project-1-e-commerce-user-behavior-analysis-1.0-SNAPSHOT.jar resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/

docker cp input/user_activity.csv resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/

docker exec -it resourcemanager /bin/bash

cd /opt/hadoop-3.2.1/share/hadoop/mapreduce/

hadoop fs -mkdir -p /input/dataset

hadoop fs -put user_activity.csv /input/dataset/

hadoop jar Project-1-e-commerce-user-behavior-analysis-1.0-SNAPSHOT.jar com.sentimentanalysis.UserActivityAnalysis /input/dataset/user_activity.csv /output

hadoop fs -ls /output1

hadoop fs -cat /output1/part-r-00000

hadoop fs -get /output1 /opt/hadoop-3.2.1/share/hadoop/mapreduce/

exit

docker cp resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/output1/ ./output1/
