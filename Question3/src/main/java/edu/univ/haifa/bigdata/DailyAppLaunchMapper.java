/* Set package */
package edu.univ.haifa.bigdata;

/* Import libraries */
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* Mapper Class */
public class DailyAppLaunchMapper extends Mapper<
        LongWritable, // Input key type
        Text,         // Input value type
        Text,         // Output key type
        Text>         // Output value type
{
    /* Private variables */
    private final Text compositeKey = new Text();
    private final Text usageCount = new Text();

    /* Implementation of the map method */
    protected void map(
            LongWritable key, // Input key type
            Text value,       // Input value type
            Context context) throws IOException, InterruptedException {

        // Convert the input value to string
        String line = value.toString();

        // Skip header
        if (line.startsWith("Date") || line.trim().isEmpty()) {
            return;
        }

        // Expected Columns: Date(0), App(1), Usage(2), Notifications(3), Times Opened(4)
        // Split the line by comma
        String[] parts = line.split(",");

        // Check if the line has enough columns
        if (parts.length >= 5) {
            try {
                String date = parts[0].trim();
                String app = parts[1].trim();
                String timesOpened = parts[4].trim();

                // Set the output key: Date + Tab + App
                // We combine them so Hadoop groups by this specific combination
                compositeKey.set(date + "\t" + app);

                // Set the output value: Just the count
                usageCount.set(timesOpened);

                // Emit the pair (date+app, count)
                context.write(compositeKey, usageCount);

            } catch (Exception e) {
                // Ignore malformed lines
            }
        }
    } // End of the map method
} // End of class DailyAppLaunchMapper