package edu.univ.haifa.bigdata;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NotificationsUsageReducer extends Reducer<Text, Text, Text, Text> {

    private final Text resultValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long totalUsage = 0;
        long totalNotifications = 0;

        for (Text val : values) {
            // Split the passed value: "Usage <tab> Notifications"
            String[] parts = val.toString().split("\t");
            if (parts.length == 2) {
                try {
                    totalUsage += Long.parseLong(parts[0]);
                    totalNotifications += Long.parseLong(parts[1]);
                } catch (NumberFormatException ignored) {
                }
            }
        }

        // Output Format: "Total Usage (minutes)    Total Notifications"
        // Example: "1200    540"
        resultValue.set(totalUsage + "\t" + totalNotifications);
        context.write(key, resultValue);
    }
}