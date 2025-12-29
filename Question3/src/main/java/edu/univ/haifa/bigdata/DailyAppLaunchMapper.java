/* Set package */
package edu.univ.haifa.bigdata;

/* Import libraries */
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* Mapper Class */
public class DailyAppLaunchMapper extends Mapper<
        LongWritable, // Input key type
        Text,         // Input value type
        Text,         // Output key type
        Text>         // Output value type
{
    /* Private variables */
    private final Text dateKey = new Text();
    private final Text appAndCountValue = new Text();

    /* Implementation of the map method */
    protected void map(
            LongWritable key, // Input key type
            Text value,       // Input value type
            Context context) throws IOException, InterruptedException {

        // Convert the input value to string
        String line = value.toString();

        // Skip header
        if (line.startsWith("Date") || line.trim().isEmpty()) {
            return;
        }

        // Expected Columns: Date(0), App(1), Usage(2), Notifications(3), Times Opened(4)
        String[] parts = line.split(",");

        if (parts.length >= 5) {
            try {
                String date = parts[0].trim();
                String app = parts[1].trim();
                String timesOpened = parts[4].trim();

                // 1. Set the Output Key: Just the Date
                // We group ONLY by date now, so the reducer sees all apps for that day together
                dateKey.set(date);

                // 2. Set the Output Value: App Name + Tab + Count
                // We need to pass the App name to the reducer so we know who the winner is
                appAndCountValue.set(app + "\t" + timesOpened);

                // Emit: Key(2023-01-01) -> Value(Facebook    150)
                context.write(dateKey, appAndCountValue);

            } catch (Exception e) {
                // Ignore malformed lines
            }
        }
    }
}