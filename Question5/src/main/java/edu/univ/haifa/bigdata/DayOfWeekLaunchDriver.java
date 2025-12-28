package edu.univ.haifa.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DayOfWeekLaunchDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: DayOfWeekLaunchDriver <input path> <output path>");
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

        Job job = Job.getInstance(conf, "Most Frequent App Launch Days");
        job.setJarByClass(DayOfWeekLaunchDriver.class);

        job.setMapperClass(DayOfWeekLaunchMapper.class);
        job.setReducerClass(DayOfWeekLaunchReducer.class);

        // Mapper outputs <Text, LongWritable>
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // Reducer outputs <Text, Text>
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}