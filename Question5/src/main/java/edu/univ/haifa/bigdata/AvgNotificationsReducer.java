package edu.univ.haifa.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class AvgNotificationsReducer extends Reducer<Text, IntWritable, Text, Text> {

    private final Text outVal = new Text();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        long sum = 0;
        long count = 0;

        for (IntWritable v : values) {
            sum += v.get();
            count++;
        }

        if (count == 0) return;

        double avg = (double) sum / (double) count;

        // Match the assignment's example format: "App - 25.3 notifications/day"
        outVal.set(String.format("- %.1f notifications/day", avg));
        context.write(key, outVal);
    }
}