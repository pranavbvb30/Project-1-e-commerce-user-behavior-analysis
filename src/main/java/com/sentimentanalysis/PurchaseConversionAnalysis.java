package com.sentimentanalysis;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PurchaseConversionAnalysis {

    // Mapper for User Engagement
    public static class UserEngagementMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text itemID = new Text();
        private Text userAction = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Split the input line by comma
            String[] columns = value.toString().split(",");

            // Skip header or malformed rows
            if (columns.length < 4 || columns[0].equals("LogID")) {
                return;
            }

            // Emit user interactions from user_activity.csv
            String itemIDStr = columns[3];  // Assuming ItemID is the 4th column
            String actionType = columns[2];  // ActionType is the 3rd column

            itemID.set(itemIDStr);
            userAction.set("action:" + actionType); // Tagging the action type
            context.write(itemID, userAction);
        }
    }

    // Mapper for Sales Transactions
    public static class SalesTransactionsMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text itemID = new Text();
        private Text saleInfo = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Split the input line by comma
            String[] columns = value.toString().split(",");

            // Skip header or malformed rows
            if (columns.length < 6 || columns[0].equals("TransactionID")) {
                return;
            }

            // Emit sales data from transactions.csv
            String itemIDStr = columns[3];  // Assuming ItemID is the 4th column
            String itemCategory = columns[2];  // Assuming ItemCategory is the 3rd column
            String quantitySold = columns[4];  // Assuming QuantitySold is the 5th column
            String revenue = columns[5];  // Assuming Revenue is the 6th column

            itemID.set(itemIDStr);
            saleInfo.set("sale:" + itemCategory + ":" + quantitySold + ":" + revenue); 
            context.write(itemID, saleInfo);
        }
    }

    // Reducer Class
    public static class ConversionRateReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int totalInteractions = 0;
            int totalSales = 0;
            double cumulativeRevenue = 0.0;
            String itemCategory = "";

            // Process values for a specific itemID
            for (Text value : values) {
                String[] segments = value.toString().split(":");
                if (segments[0].equals("action")) {
                    totalInteractions++;
                } else if (segments[0].equals("sale")) {
                    totalSales++;
                    itemCategory = segments[1];  // Extracting item category from sale info
                    cumulativeRevenue += Double.parseDouble(segments[3]);  // Summing the revenue
                }
            }

            // Calculate conversion rate
            double conversionRate = (totalInteractions == 0) ? 0 : (double) totalSales / totalInteractions;

            // Output: ItemID, ItemCategory, ConversionRate, CumulativeRevenue
            context.write(key, new Text(itemCategory + ", ConversionRate: " + conversionRate + ", CumulativeRevenue: " + cumulativeRevenue));
        }
    }

    // Driver Class
    public static void main(String[] args) throws Exception {
        // Check number of input arguments
        if (args.length != 3) {
            System.err.println("Usage: PurchaseConversionAnalysis <user_activity_input> <transactions_input> <output>");
            System.exit(-1);
        }

        // Create Hadoop job configuration
        Configuration config = new Configuration();
        Job job = Job.getInstance(config, "Purchase Conversion Rate Analysis");

        // Set job jar and main class
        job.setJarByClass(PurchaseConversionAnalysis.class);

        // Set input formats and mappers
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, UserEngagementMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, SalesTransactionsMapper.class);

        // Set output format and reducer
        job.setReducerClass(ConversionRateReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // Execute the job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
