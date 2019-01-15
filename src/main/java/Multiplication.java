import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Multiplication {
	public static class CooccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//input: movieB \t movieA=relation
			String[] moiveB_movieARelation = value.toString().trim().split("\t");
			String outputKey = moiveB_movieARelation[0];
			String outputValue = moiveB_movieARelation[1];
			context.write(new Text(outputKey), new Text(outputValue));
		}
	}

	public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//input: user,movie,rating
			String[] user_movie_rating = value.toString().trim().split(",");
			if (user_movie_rating.length != 3) {
				return;
			}

			String outputKey = user_movie_rating[1];
			String outputValue = user_movie_rating[0] + ":" + user_movie_rating[2];
			context.write(new Text(outputKey), new Text(outputValue));
		}
	}

	public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			//key = movieB
			//value = <movieA=relation, movieC=relation, userA:rating, userB:rating...>
			Map<String, Double> relation = new HashMap<String, Double>();
			Map<String, Double> rating = new HashMap<String, Double>();

			for (Text value : values) {
				if (value.toString().contains("=")) {
					String[] movieA_relation = value.toString().trim().split("=");
					relation.put(movieA_relation[0], Double.parseDouble(movieA_relation[1]));
				} else if (value.toString().contains(":")) {
					String[] user_rating = value.toString().trim().split(":");
					rating.put(user_rating[0], Double.parseDouble(user_rating[1]));
				}
			}

			for (String movieA : relation.keySet()) {
				for (String user : rating.keySet()) {
					String outputKey = user + ":" + movieA;
					Double outputValue = relation.get(movieA) * rating.get(user);
					context.write(new Text(outputKey), new DoubleWritable(outputValue));
				}
			}
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(Multiplication.class);

		ChainMapper.addMapper(job, CooccurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
		ChainMapper.addMapper(job, RatingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

		job.setMapperClass(CooccurrenceMapper.class);
		job.setMapperClass(RatingMapper.class);

		job.setReducerClass(MultiplicationReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CooccurrenceMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

		TextOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.waitForCompletion(true);
	}
}
