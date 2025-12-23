package edu.univ.haifa.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ScreenTimeAnalysis {

    // --- JOB 1: SUM USAGE ---
    public static class UsageMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final Text appName = new Text();
        private final IntWritable minutes = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("Date") || line.trim().isEmpty()) return; // Skip Header

            String[] parts = line.split(",");
            if (parts.length >= 3) {
                try {
                    appName.set(parts[1].trim());
                    minutes.set(Integer.parseInt(parts[2].trim()));
                    context.write(appName, minutes);
                } catch (NumberFormatException ignored) {}
            }
        }
    }

    public static class SumReducer extends Reducer<Text, IntWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (IntWritable val : values) sum += val.get();
            context.write(key, new LongWritable(sum));
        }
    }

    // --- JOB 2: SORT & TOP 5 ---
    public static class SortMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length == 2) {
                context.write(new LongWritable(Long.parseLong(parts[1])), new Text(parts[0]));
            }
        }
    }

    public static class Top5Reducer extends Reducer<LongWritable, Text, Text, Text> {
        private int count = 0;

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text app : values) {
                if (count < 5) {
                    count++;
                    // Exact format: "1. Instagram - 1200 minutes"
                    String formattedKey = count + ". " + app.toString();
                    String formattedValue = " - " + key.get() + " minutes";
                    context.write(new Text(formattedKey), new Text(formattedValue));
                }
            }
        }
    }

    // Descending Comparator for sorting screen time
    public static class DescendingComparator extends WritableComparator {
        protected DescendingComparator() { super(LongWritable.class, true); }
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        Path tempPath = new Path("temp_screentime");

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(tempPath)) fs.delete(tempPath, true);
        if (fs.exists(outputPath)) fs.delete(outputPath, true);

        // Job 1
        Job job1 = Job.getInstance(conf, "Sum Usage");
        job1.setJarByClass(ScreenTimeAnalysis.class);
        job1.setMapperClass(UsageMapper.class);
        job1.setReducerClass(SumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, tempPath);

        if (job1.waitForCompletion(true)) {
            // Job 2
            Job job2 = Job.getInstance(conf, "Top 5");
            job2.setJarByClass(ScreenTimeAnalysis.class);
            job2.setMapperClass(SortMapper.class);
            job2.setReducerClass(Top5Reducer.class);
            job2.setSortComparatorClass(DescendingComparator.class);
            job2.setNumReduceTasks(1); // Required for global Top 5
            job2.setMapOutputKeyClass(LongWritable.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2, tempPath);
            FileOutputFormat.setOutputPath(job2, outputPath);
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}