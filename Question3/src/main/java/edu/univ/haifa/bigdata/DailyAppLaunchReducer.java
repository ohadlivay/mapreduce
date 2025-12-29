/* Set package */
package edu.univ.haifa.bigdata;

/* Import libraries */
import java.io.IOException;
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
    private final Text finalKey = new Text();
    private final Text resultValue = new Text();

    /* Implementation of the reduce method */
    protected void reduce(
            Text key,               // Input key (Date + Tab + App)
            Iterable<Text> values,  // Input values (Counts)
            Context context) throws IOException, InterruptedException {

        long sum = 0;

        // Iterate over the set of values and sum them
        for (Text val : values) {
            try {
                sum += Long.parseLong(val.toString());
            } catch (NumberFormatException ignored) {
                // Ignore malformed numbers
            }
        }

        // Split the composite key to separate Date and App for formatting
        String[] parts = key.toString().split("\t");

        if (parts.length == 2) {
            String date = parts[0];
            String app = parts[1];

            // Set output key to just the Date
            finalKey.set(date);

            // Set formatted result: "- AppName (Count times)"
            resultValue.set(String.format("- %s (%d times)", app, sum));

            // Emit the pair
            context.write(finalKey, resultValue);
        }

    } // End reduce method
} // End of class DailyAppLaunchReducer