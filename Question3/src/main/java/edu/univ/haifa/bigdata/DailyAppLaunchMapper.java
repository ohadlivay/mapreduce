/* Set package */
package edu.univ.haifa.bigdata;

/* Import libraries */
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DailyAppLaunchMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text dateKey = new Text();
    private final Text appAndCountValue = new Text();

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // Skip header
        if (line.startsWith("Date") || line.trim().isEmpty()) {
            return;
        }

        // Columns: Date(0), App(1), Usage(2), Notifications(3), Times Opened(4)
        String[] parts = line.split(",");

        if (parts.length >= 5) {
            try {
                String date = parts[0].trim();
                String app = parts[1].trim();
                String timesOpened = parts[4].trim();

                // Key: Date (so we process one full day at a time)
                dateKey.set(date);

                // Value: AppName + Tab + Count (Data needed for aggregation)
                appAndCountValue.set(app + "\t" + timesOpened);

                context.write(dateKey, appAndCountValue);

            } catch (Exception e) {
                // Ignore malformed lines
            }
        }
    }
}