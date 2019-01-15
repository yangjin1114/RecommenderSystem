import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Normalize {

    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //value: movieA:movieB \t relation
            String[] line = value.toString().trim().split("\t");
            String outputKey = line[0].split(":")[0];
            String outputValue = line[0].split(":")[1] + "=" + line[1];

            //outputKey: movieA
            //outputValue: movieB=relation
            context.write(new Text(outputKey), new Text(outputValue));
        }
    }

    public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {
        // reduce method
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //sum -> denominator
            //context write -> transpose
            int sum = 0;
            Map<String, Integer> map = new HashMap<String, Integer>();
            for (Text value : values) {
                String[] inputValue = value.toString().trim().split("=");
                String movieB = inputValue[0];
                int relation = Integer.parseInt(inputValue[1]);
                sum += relation;
                map.put(movieB, relation);
            }

            for (String movieB : map.keySet()) {
                String outputKey = movieB;
                String outputValue = key.toString() + (double)map.get(movieB) / sum;
                context.write(new Text(outputKey), new Text(outputValue));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);

        job.setJarByClass(Normalize.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
