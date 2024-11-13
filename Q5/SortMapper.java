import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    private IntWritable number = new IntWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int num = Integer.parseInt(value.toString());
        number.set(num);
        context.write(number, new IntWritable(1)); // Using 1 as a dummy value
    }
}
