package edu.univ.haifa.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NotificationsUsageDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: NotificationsUsageDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        // Cleanup output directory if it exists
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf, "Notifications vs Usage Analysis");
        job.setJarByClass(NotificationsUsageDriver.class);

        job.setMapperClass(NotificationsUsageMapper.class);
        // Using the Reducer as a Combiner is safe here because we are just summing values
        job.setCombinerClass(NotificationsUsageReducer.class);
        job.setReducerClass(NotificationsUsageReducer.class);

        // Mapper and Reducer both output Text Keys and Text Values
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}