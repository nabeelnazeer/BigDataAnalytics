import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxTemp = Integer.MIN_VALUE;

        // Iterate over all temperatures to find the maximum
        for (IntWritable value : values) {
            maxTemp = Math.max(maxTemp, value.get());
        }
        context.write(key, new IntWritable(maxTemp));
    }
}
