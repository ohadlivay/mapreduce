package edu.univ.haifa.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvgNotificationsDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: AvgNotificationsDriver <input_path> <output_path>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) fs.delete(outputPath, true);

        Job job = Job.getInstance(conf, "Average Notifications per App");
        job.setJarByClass(AvgNotificationsDriver.class);

        job.setMapperClass(AvgNotificationsMapper.class);
        job.setReducerClass(AvgNotificationsReducer.class);

        // Mapper output
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Final output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}