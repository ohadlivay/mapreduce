package edu.univ.haifa.bigdata;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DayOfWeekLaunchReducer extends Reducer<Text, LongWritable, Text, Text> {

    private final Text resultValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;

        // Sum the "Times Opened" for this specific day of the week
        for (LongWritable val : values) {
            sum += val.get();
        }

        // Format output: "- 2500 launches"
        resultValue.set("- " + sum + " launches");
        context.write(key, resultValue);
    }
}