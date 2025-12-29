/* Set package */
package edu.univ.haifa.bigdata;

/* Import libraries */
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* Mapper Class */
public class NotificationsUsageMapper extends Mapper<
        LongWritable, // Input key type
        Text,         // Input value type
        Text,         // Output key type
        Text>         // Output value type
{
    /* Private variables */
    private final Text appKey = new Text();
    private final Text statsValue = new Text();

    /* Implementation of the map method */
    protected void map(
            LongWritable key, // Input key type
            Text value,       // Input value type
            Context context) throws IOException, InterruptedException {

        System.out.print("Mapping...");

        // Convert the input value to string
        String line = value.toString();

        // Skip header row
        if (line.startsWith("Date") || line.trim().isEmpty()) {
            return;
        }

        // Expected Columns: Date(0), App(1), Usage(2), Notifications(3), Times Opened(4)
        // Split the line by comma
        String[] parts = line.split(",");

        // Check if the line has enough columns
        if (parts.length >= 4) {
            try {
                String app = parts[1].trim();
                String usageStr = parts[2].trim();
                String notifStr = parts[3].trim();

                // Validate they are numbers before writing
                Integer.parseInt(usageStr);
                Integer.parseInt(notifStr);

                // Set the output key
                appKey.set(app);

                // Set the output value
                // Combine Usage and Notifications with a separator (TAB)
                statsValue.set(usageStr + "\t" + notifStr);

                // context.write(..) is used to emit (key, value) pairs
                context.write(appKey, statsValue);

            } catch (NumberFormatException ignored) {
                // Ignore lines with invalid numbers
            }
        }
    } // End of the map method
} // End of class NotificationsUsageMapper