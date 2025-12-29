/* Set package */
package edu.univ.haifa.bigdata;

/* Import libraries */
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* Reducer Class */
public class DayOfWeekLaunchReducer extends Reducer<
        Text,           // Input key type
        LongWritable,   // Input value type
        Text,           // Output key type
        LongWritable>   // Output value type
{
    /* Private variables */
    private final LongWritable resultValue = new LongWritable();

    /* Implementation of the reduce method */
    protected void reduce(
            Text key,                       // Input key
            Iterable<LongWritable> values,  // Input values
            Context context) throws IOException, InterruptedException {

        long sum = 0;

        // Sum the "Times Opened" for this specific day of the week
        for (LongWritable val : values) {
            sum += val.get();
        }

        // Set the output value (Raw number)
        resultValue.set(sum);

        // context.write(..) is used to emit (key, value) pairs
        context.write(key, resultValue);

    } // End reduce method
} // End of class DayOfWeekLaunchReducer