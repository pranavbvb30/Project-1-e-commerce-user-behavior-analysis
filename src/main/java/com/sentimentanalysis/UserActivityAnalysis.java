package com.sentimentanalysis;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class UserActivityAnalysis {

    // Mapper Class
    public static class UserActivityMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable countOne = new IntWritable(1);
        private Text userIdentifier = new Text();

        @Override
        protected void map(LongWritable lineKey, Text lineValue, Context context) throws IOException, InterruptedException {
            // Split the line (assuming CSV format with comma as separator)
            String[] recordFields = lineValue.toString().split(",");
            if (recordFields.length > 1) {
                // Extract UserID (assuming it's the second column)
                String userId = recordFields[1];
                userIdentifier.set(userId);
                // Emit UserID with a count of 1 for each activity
                context.write(userIdentifier, countOne);
            }
        }
    }

    // Reducer Class
    public static class UserActivityReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Map<String, Integer> activityCountMap = new HashMap<>();

        @Override
        protected void reduce(Text userKey, Iterable<IntWritable> activityValues, Context context)
                throws IOException, InterruptedException {
            int totalCount = 0;
            // Sum up all the interactions for the user
            for (IntWritable value : activityValues) {
                totalCount += value.get();
            }
            activityCountMap.put(userKey.toString(), totalCount);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Use a TreeMap to store users by their interaction count in sorted order
            TreeMap<Integer, String> sortedUserMap = new TreeMap<>();

            for (Map.Entry<String, Integer> entry : activityCountMap.entrySet()) {
                sortedUserMap.put(entry.getValue(), entry.getKey());
                // Maintain only top 10 entries in the sorted map
                if (sortedUserMap.size() > 10) {
                    sortedUserMap.remove(sortedUserMap.firstKey()); // Remove the smallest count
                }
            }

            // Write the top 10 users to context in descending order
            for (Map.Entry<Integer, String> entry : sortedUserMap.descendingMap().entrySet()) {
                context.write(new Text(entry.getValue()), new IntWritable(entry.getKey()));
            }
        }
    }

    // Driver code
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "User Activity Analysis");

        job.setJarByClass(UserActivityAnalysis.class);
        job.setMapperClass(UserActivityMapper.class);
        job.setReducerClass(UserActivityReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));  // Input path
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  // Output path

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
