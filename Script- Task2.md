mvn clean install

docker cp target/Project-1-e-commerce-user-behavior-analysis-1.0-SNAPSHOT.jar resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/

docker cp input/user_activity.csv resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/

docker cp input/transactions.csv resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/

docker exec -it resourcemanager /bin/bash

cd /opt/hadoop-3.2.1/share/hadoop/mapreduce/

hadoop fs -mkdir -p /input/datasets

hadoop fs -put transactions.csv /input/datasets/

hadoop fs -put user_activity.csv /input/datasets/

hadoop jar Project-1-e-commerce-user-behavior-analysis-1.0-SNAPSHOT.jar com.sentimentanalysis.PurchaseConversionAnalysis /input/datasets/user_activity.csv /input/datasets/transactions.csv /output2nd

hadoop fs -ls /output2nd

hadoop fs -cat /output2nd/part-r-00000

hadoop fs -get /output2nd /opt/hadoop-3.2.1/share/hadoop/mapreduce/

exit

docker cp d5973f6bf6fc:/opt/hadoop-3.2.1/share/hadoop/mapreduce/output2nd/ ./output2nd/