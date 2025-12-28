package edu.univ.haifa.bigdata;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DailyAppLaunchReducer extends Reducer<Text, Text, Text, Text> {

    private final Text resultValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Map to store sum of launches per App for this specific Date
        Map<String, Long> appCounts = new HashMap<>();

        // 1. Iterate and Sum (Aggregation Step)
        for (Text val : values) {
            String[] parts = val.toString().split("\t");
            if (parts.length == 2) {
                String appName = parts[0];
                try {
                    long count = Long.parseLong(parts[1]);
                    appCounts.put(appName, appCounts.getOrDefault(appName, 0L) + count);
                } catch (NumberFormatException ignored) {
                }
            }
        }

        // 2. Find the Max (Selection Step)
        String maxApp = null;
        long maxCount = -1;

        for (Map.Entry<String, Long> entry : appCounts.entrySet()) {
            if (entry.getValue() > maxCount) {
                maxCount = entry.getValue();
                maxApp = entry.getKey();
            }
        }

        // 3. Write Output
        if (maxApp != null) {
            // Format: "- AppName (Count times)"
            // The Key (Date) is written automatically by context.write
            resultValue.set(String.format("- %s (%d times)", maxApp, maxCount));
            context.write(key, resultValue);
        }
    }
}