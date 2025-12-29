/* Set package */
package edu.univ.haifa.bigdata;

/* Import libraries */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/* Driver class */
public class NotificationsUsageDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        /* variables */
        int exitCode;
        Path inputPath;
        Path outputPath;

        // Check arguments consistency
        if (args.length != 2) {
            System.err.println("Usage: NotificationsUsageDriver <input path> <output path>");
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
        job.setJobName("Notifications vs Usage Analysis");

        // Set path of the input file/folder for this job
        FileInputFormat.addInputPath(job, inputPath);

        // Set path of the output folder for this job
        FileOutputFormat.setOutputPath(job, outputPath);

        // Set input format
        job.setInputFormatClass(TextInputFormat.class);

        // Set job output format
        job.setOutputFormatClass(TextOutputFormat.class);

        // Specify the class of the Driver for this job
        job.setJarByClass(NotificationsUsageDriver.class);

        // Set mapper class
        job.setMapperClass(NotificationsUsageMapper.class);

        // Set map output key and value classes
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Set reducer class
        job.setReducerClass(NotificationsUsageReducer.class);

        // Set combiner class
        // Using the Reducer as a Combiner is safe here because we are just summing values
        job.setCombinerClass(NotificationsUsageReducer.class);

        // Set reduce output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Execute the job and wait for completion
        if (job.waitForCompletion(true) == true)
            exitCode = 0;
        else
            exitCode = 1;

        return exitCode;
    }

    /* main method of the driver class */
    public static void main(String args[]) throws Exception {
        System.out.print("Driver starting");
        /* Exploit the ToolRunner class to "configure" and run the Hadoop application */
        int res = ToolRunner.run(new Configuration(), new NotificationsUsageDriver(), args);
        System.out.print("Driver finished");
        System.exit(res);
    }
}