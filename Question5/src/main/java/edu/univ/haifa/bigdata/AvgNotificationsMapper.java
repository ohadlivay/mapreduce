package edu.univ.haifa.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

public class AvgNotificationsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final Text appKey = new Text();
    private final IntWritable notifVal = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) return;

        // Skip header (works for "Date,App,Usage...,Notifications,...")
        if (line.startsWith("Date")) return;

        String[] parts = line.split(",");
        // Expected columns: Date, App, Usage (minutes), Notifications, Times Opened
        if (parts.length < 5) return;

        String app = parts[1].trim();
        String notifStr = parts[3].trim(); // Notifications = index 3

        int notifications;
        try {
            notifications = Integer.parseInt(notifStr);
        } catch (NumberFormatException e) {
            return;
        }

        appKey.set(app);
        notifVal.set(notifications);
        context.write(appKey, notifVal);
    }
}