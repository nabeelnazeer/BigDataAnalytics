import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class MatrixMultiplication {

    public static class MatrixMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            String matrix = itr.nextToken(); // Matrix name (A or B)
            int row = Integer.parseInt(itr.nextToken());
            int col = Integer.parseInt(itr.nextToken());
            int val = Integer.parseInt(itr.nextToken());

            Configuration conf = context.getConfiguration();
            int n = Integer.parseInt(conf.get("n")); // number of columns in Matrix A / rows in Matrix B
            int m = Integer.parseInt(conf.get("m")); // number of columns in Matrix B

            if (matrix.equals("A")) {
                for (int k = 0; k < m; k++) {
                    context.write(new Text(row + "," + k), new Text("A," + col + "," + val));
                }
            } else {
                for (int i = 0; i < n; i++) {
                    context.write(new Text(i + "," + col), new Text("B," + row + "," + val));
                }
            }
        }
    }

    public static class MatrixReducer extends Reducer<Text, Text, Text, IntWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int[] A = new int[1000]; // Assuming maximum of 1000 elements
            int[] B = new int[1000];
            for (Text val : values) {
                String[] parts = val.toString().split(",");
                if (parts[0].equals("A")) {
                    A[Integer.parseInt(parts[1])] = Integer.parseInt(parts[2]);
                } else {
                    B[Integer.parseInt(parts[1])] = Integer.parseInt(parts[2]);
                }
            }

            int result = 0;
            for (int i = 0; i < 1000; i++) {
                result += A[i] * B[i];
            }
            context.write(key, new IntWritable(result));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("n", "2"); // Number of columns in Matrix A (rows in Matrix B)
        conf.set("m", "2"); // Number of columns in Matrix B

        Job job = Job.getInstance(conf, "matrix multiplication");
        job.setJarByClass(MatrixMultiplication.class);
        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
