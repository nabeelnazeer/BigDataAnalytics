import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text year = new Text();
    private IntWritable temperature = new IntWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split the input line into words
        String[] tokens = value.toString().split("\\s+");
        
        if (tokens.length == 2) {
            // Extract year and temperature
            year.set(tokens[0]); // Year as the key
            temperature.set(Integer.parseInt(tokens[1])); // Temperature as the value
            context.write(year, temperature);
        }
    }
}
