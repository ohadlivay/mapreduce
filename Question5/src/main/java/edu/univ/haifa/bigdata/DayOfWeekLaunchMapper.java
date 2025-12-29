/* Set package */
package edu.univ.haifa.bigdata;

/* Import libraries */
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.util.Locale;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* Mapper Class */
public class DayOfWeekLaunchMapper extends Mapper<
        LongWritable, // Input key type
        Text,         // Input value type
        Text,         // Output key type
        LongWritable> // Output value type
{
    /* Private variables */
    private final Text dayKey = new Text();
    private final LongWritable launchesValue = new LongWritable();

    // Formatter for the input date format "yyyy-MM-dd"
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

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
        // Split the line by comma
        String[] parts = line.split(",");

        // Check if the line has enough columns
        if (parts.length >= 5) {
            try {
                String dateStr = parts[0].trim();
                String timesOpenedStr = parts[4].trim();

                // 1. Parse the date
                LocalDate date = LocalDate.parse(dateStr, DATE_FORMATTER);

                // 2. Get Day of Week (e.g., "Monday")
                String dayOfWeek = date.getDayOfWeek().getDisplayName(TextStyle.FULL, Locale.ENGLISH);

                // 3. Parse launch count
                long launches = Long.parseLong(timesOpenedStr);

                // Set the output key
                dayKey.set(dayOfWeek);

                // Set the output value
                launchesValue.set(launches);

                // context.write(..) is used to emit (key, value) pairs
                context.write(dayKey, launchesValue);

            } catch (Exception e) {
                // Ignore parsing errors
            }
        }
    } // End of the map method
} // End of class DayOfWeekLaunchMapper