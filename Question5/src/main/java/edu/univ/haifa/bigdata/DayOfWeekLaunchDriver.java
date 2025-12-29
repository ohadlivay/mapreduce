/* Set package */
package edu.univ.haifa.bigdata;

/* Import libraries */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/* Driver class */
public class DayOfWeekLaunchDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        /* variables */
        int exitCode;
        Path inputPath;
        Path outputPath;

        // Check arguments consistency
        if (args.length != 2) {
            System.err.println("Usage: DayOfWeekLaunchDriver <input path> <output path>");
            return -1;
        }

        // Parse parameters
        inputPath = new Path(args[0]);
        outputPath = new Path(args[1]);

        // Define and configure a new job
        Configuration conf = this.getConf();

        // Cleanup output directory if it exists
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf);

        // Assign a name to the job
        job.setJobName("Most Frequent App Launch Days");

        // Set path of the input file/folder for this job
        FileInputFormat.addInputPath(job, inputPath);

        // Set path of the output folder for this job
        FileOutputFormat.setOutputPath(job, outputPath);

        // Set input format
        job.setInputFormatClass(TextInputFormat.class);

        // Set job output format
        job.setOutputFormatClass(TextOutputFormat.class);

        // Specify the class of the Driver for this job
        job.setJarByClass(DayOfWeekLaunchDriver.class);

        // Set mapper class
        job.setMapperClass(DayOfWeekLaunchMapper.class);

        // Set map output key and value classes
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // Set combiner class
        // Safe to use because Reducer is now associative (Sum) and types match
        job.setCombinerClass(DayOfWeekLaunchReducer.class);

        // Set reducer class
        job.setReducerClass(DayOfWeekLaunchReducer.class);

        // Set reduce output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // Execute the job and wait for completion
        if (job.waitForCompletion(true) == true)
            exitCode = 0;
        else
            exitCode = 1;

        return exitCode;
    }

    /* main method of the driver class */
    public static void main(String args[]) throws Exception {
        /* Exploit the ToolRunner class to "configure" and run the Hadoop application */
        int res = ToolRunner.run(new Configuration(), new DayOfWeekLaunchDriver(), args);
        System.exit(res);
    }
}