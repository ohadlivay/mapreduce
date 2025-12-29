/* Set package */
package edu.univ.haifa.bigdata;

/* Import libraries */
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* Reducer Class */
public class NotificationsUsageReducer extends Reducer<
        Text,           // Input key type
        Text,           // Input value type
        Text,           // Output key type
        Text>           // Output value type
{
    /* Private variables */
    private final Text resultValue = new Text();

    /* Implementation of the reduce method */
    protected void reduce(
            Text key,               // Input key
            Iterable<Text> values,  // Input values
            Context context) throws IOException, InterruptedException {


        long totalUsage = 0;
        long totalNotifications = 0;

        // Iterate over the set of values and sum them
        for (Text val : values) {
            // Split the passed value: "Usage <tab> Notifications"
            String[] parts = val.toString().split("\t");

            if (parts.length == 2) {
                try {
                    totalUsage += Long.parseLong(parts[0]);
                    totalNotifications += Long.parseLong(parts[1]);
                } catch (NumberFormatException ignored) {
                    // Ignore malformed numbers
                }
            }
        }

        // Output Format: "Total Usage (minutes)    Total Notifications"
        // Example: "1200    540"
        resultValue.set(totalUsage + "\t" + totalNotifications);

        // context.write(..) is used to emit (key, value) pairs
        context.write(key, resultValue);

    } // End reduce method
} // End of class NotificationsUsageReducer