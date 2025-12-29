/* Set package */
package edu.univ.haifa.bigdata;

/* Import libraries */
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DailyAppLaunchReducer extends Reducer<Text, Text, Text, Text> {

    private final Text resultValue = new Text();

    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        // 1. Create a map to aggregate sums for each app on this specific day
        Map<String, Long> appSums = new HashMap<>();

        // 2. Iterate through all incoming logs for this day
        for (Text val : values) {
            String[] parts = val.toString().split("\t");
            if (parts.length == 2) {
                String appName = parts[0];
                try {
                    long count = Long.parseLong(parts[1]);

                    // Add to the existing sum for this app (or initialize if new)
                    appSums.put(appName, appSums.getOrDefault(appName, 0L) + count);

                } catch (NumberFormatException ignored) { }
            }
        }

        // 3. Find the app with the maximum AGGREGATED value
        String maxAppName = "Unknown";
        long maxCount = -1;

        for (Map.Entry<String, Long> entry : appSums.entrySet()) {
            if (entry.getValue() > maxCount) {
                maxCount = entry.getValue();
                maxAppName = entry.getKey();
            }
        }

        // 4. Write the result for this day
        if (maxCount != -1) {
            resultValue.set(String.format("- %s (%d times)", maxAppName, maxCount));
            context.write(key, resultValue);
        }
    }
}