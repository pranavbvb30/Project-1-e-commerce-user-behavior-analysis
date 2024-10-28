mvn clean install

docker cp target/Project-1-e-commerce-user-behavior-analysis-1.0-SNAPSHOT.jar resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/

docker cp input/transactions.csv resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/

docker exec -it resourcemanager /bin/bash

cd /opt/hadoop-3.2.1/share/hadoop/mapreduce/

hadoop fs -mkdir -p /input/datasets

hadoop fs -put transactions.csv /input/datasets/

hadoop jar Project-1-e-commerce-user-behavior-analysis-1.0-SNAPSHOT.jar com.sentimentanalysis.PurchaseBehaviorAnalysis  /input/datasets/transactions.csv /output3rd

hadoop fs -ls /output3rd

hadoop fs -cat /output3rd/part-r-00000

hadoop fs -get /output3rd /opt/hadoop-3.2.1/share/hadoop/mapreduce/

docker cp 0092ddd079ed:/opt/hadoop-3.2.1/share/hadoop/mapreduce/output3rd/ ./output3rd/