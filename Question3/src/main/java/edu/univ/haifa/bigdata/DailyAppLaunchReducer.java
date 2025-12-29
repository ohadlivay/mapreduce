/* Set package */
package edu.univ.haifa.bigdata;

/* Import libraries */
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* Reducer Class */
public class DailyAppLaunchReducer extends Reducer<
        Text,           // Input key type (Date)
        Text,           // Input value type (App + Tab + Count)
        Text,           // Output key type (Date)
        Text>           // Output value type (Max App info)
{
    /* Private variables */
    private final Text resultValue = new Text();

    /* Implementation of the reduce method */
    protected void reduce(
            Text key,               // Input key (Date)
            Iterable<Text> values,  // Input values (List of "App\tCount")
            Context context) throws IOException, InterruptedException {

        String maxAppName = "Unknown";
        long maxCount = -1;

        // Iterate over all apps for this specific day
        for (Text val : values) {
            String[] parts = val.toString().split("\t");

            if (parts.length == 2) {
                String currentApp = parts[0];
                try {
                    long currentCount = Long.parseLong(parts[1]);

                    // Check if this is the new maximum
                    if (currentCount > maxCount) {
                        maxCount = currentCount;
                        maxAppName = currentApp;
                    }
                } catch (NumberFormatException ignored) {
                    // Ignore parsing errors
                }
            }
        }

        // If we found valid data, write the winner
        if (maxCount != -1) {
            // Format: "- AppName (MaxCount times)"
            resultValue.set(String.format("- %s (%d times)", maxAppName, maxCount));
            context.write(key, resultValue);
        }

    }
}