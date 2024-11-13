import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private static final IntWritable dummyValue = new IntWritable(1);

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // Just output the key, which is already sorted since Mapper sorts keys
        for (IntWritable val : values) {
            context.write(key, dummyValue);
        }
    }
}
