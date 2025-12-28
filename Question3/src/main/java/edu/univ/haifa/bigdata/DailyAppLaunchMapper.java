package edu.univ.haifa.bigdata;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DailyAppLaunchMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text dateKey = new Text();
    private final Text appAndCount = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // Skip header
        if (line.startsWith("Date") || line.trim().isEmpty()) {
            return;
        }

        // Expected Columns: Date(0), App(1), Usage(2), Notifications(3), Times Opened(4)
        // Using split with -1 to handle empty trailing columns if necessary
        String[] parts = line.split(",");

        if (parts.length >= 5) {
            try {
                String date = parts[0].trim();
                String app = parts[1].trim();
                String timesOpened = parts[4].trim();

                // Key: Date (e.g., "2024-08-07")
                dateKey.set(date);

                // Value: "App<tab>Count" (e.g., "Instagram\t57")
                appAndCount.set(app + "\t" + timesOpened);

                context.write(dateKey, appAndCount);

            } catch (Exception e) {
                // Ignore malformed lines
            }
        }
    }
}