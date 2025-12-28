package edu.univ.haifa.bigdata;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NotificationsUsageMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text appKey = new Text();
    private final Text statsValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // Skip header row
        if (line.startsWith("Date") || line.trim().isEmpty()) {
            return;
        }

        // Expected Columns: Date(0), App(1), Usage(2), Notifications(3), Times Opened(4)
        String[] parts = line.split(",");

        if (parts.length >= 4) {
            try {
                String app = parts[1].trim();
                String usageStr = parts[2].trim();
                String notifStr = parts[3].trim();

                // Validate they are numbers before writing
                Integer.parseInt(usageStr);
                Integer.parseInt(notifStr);

                appKey.set(app);
                // Combine Usage and Notifications with a separator (TAB)
                statsValue.set(usageStr + "\t" + notifStr);

                context.write(appKey, statsValue);

            } catch (NumberFormatException ignored) {
                // Ignore lines with invalid numbers
            }
        }
    }
}