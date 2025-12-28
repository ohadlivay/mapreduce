package edu.univ.haifa.bigdata;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.util.Locale;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DayOfWeekLaunchMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private final Text dayKey = new Text();
    private final LongWritable launchesValue = new LongWritable();
    // Formatter for the input date format "yyyy-MM-dd"
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // Skip header
        if (line.startsWith("Date") || line.trim().isEmpty()) {
            return;
        }

        // Expected Columns: Date(0), App(1), Usage(2), Notifications(3), Times Opened(4)
        String[] parts = line.split(",");

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

                dayKey.set(dayOfWeek);
                launchesValue.set(launches);

                context.write(dayKey, launchesValue);

            } catch (Exception e) {
                // Ignore parsing errors
            }
        }
    }
}