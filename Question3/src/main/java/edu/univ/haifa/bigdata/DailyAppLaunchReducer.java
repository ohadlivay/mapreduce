/* Set package */
package edu.univ.haifa.bigdata;

/* Import libraries */
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* Reducer Class */
public class DailyAppLaunchReducer extends Reducer<
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

        // Map to store sum of launches per App for this specific Date
        Map<String, Long> appCounts = new HashMap<>();

        // Variables for tracking the max
        String maxApp = null;
        long maxCount = -1;

        // Iterate over the set of values and sum them
        for (Text val : values) {
            String[] parts = val.toString().split("\t");

            if (parts.length == 2) {
                String appName = parts[0];
                try {
                    long count = Long.parseLong(parts[1]);
                    // Aggregate counts
                    appCounts.put(appName, appCounts.getOrDefault(appName, 0L) + count);
                } catch (NumberFormatException ignored) {
                    // Ignore malformed numbers
                }
            }
        }

        // Find the Max (Selection Step)
        for (Map.Entry<String, Long> entry : appCounts.entrySet()) {
            if (entry.getValue() > maxCount) {
                maxCount = entry.getValue();
                maxApp = entry.getKey();
            }
        }

        // Emit the pair (Date, Formatted Result)
        if (maxApp != null) {
            // Format: "- AppName (Count times)"
            resultValue.set(String.format("- %s (%d times)", maxApp, maxCount));

            // context.write(..) is used to emit (key, value) pairs
            context.write(key, resultValue);
        }

    } // End reduce method
} // End of class DailyAppLaunchReducer