package hw1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.HashMap;

public class Q2 {

	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {

		String pair = "";

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("\t");
			if (line.length == 2) {
				int AId = Integer.parseInt(line[0]);
				String[] friends = line[1].split(",");
				for (String friend : friends) {

					int BId = Integer.parseInt(friend);
					if (AId <= BId) {
						pair = AId + "," + BId;
					} else {
						pair = BId + "," + AId;
					}
					context.write(new Text(pair), new Text(line[1]));

				}
			}
		}
	}

	public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
		Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Boolean> map = new HashMap<>();
			long count = 0;
			for (Text value : values) {
				String[] friends = value.toString().split(",");
				for (String friend : friends) {
					if (map.containsKey(friend)) {
						count++;
					} else {
						map.put(friend, true);
					}
				}
			}

			context.write(key, new Text(count + ""));
		}

	}

	public static class Map2 extends Mapper<LongWritable, Text, IntWritable, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line[] = value.toString().split("\t");
			if (line.length == 2) {
				context.write(new IntWritable(-1 * Integer.parseInt(line[1])), new Text(line[0] + "\t" + line[1]));
			}
		}

	}

	public static class Reduce2 extends Reducer<IntWritable, Text, Text, Text> {
		int count = 0;

		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				if (count < 1) {
					String[] line = value.toString().split("\t");
					context.write(new Text(line[0]), new Text(line[1] + ""));
					count++;
				} else {
					break;
				}
			}
		}

	}

	public static void main(String args[]) throws Exception {

		Configuration confA = new Configuration();
		String[] otherargs = new GenericOptionsParser(confA, args).getRemainingArgs();

		Job jobA = Job.getInstance(confA, "Max Number of Mutual Friends Stage 1");
		jobA.setJarByClass(Q2.class);
		jobA.setMapperClass(Map1.class);
		jobA.setReducerClass(Reduce1.class);

		jobA.setMapOutputKeyClass(Text.class);
		jobA.setMapOutputValueClass(Text.class);
		jobA.setOutputKeyClass(Text.class);
		jobA.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(jobA, new Path(otherargs[0]));
		FileOutputFormat.setOutputPath(jobA, new Path(otherargs[1]));
		boolean mapreduce = jobA.waitForCompletion(true);

		if (mapreduce) {
			Configuration confB = new Configuration();

			Job jobB = Job.getInstance(confB, "Max Number of Mutual Friends Stage 2");
			jobB.setJarByClass(Q2.class);

			jobB.setMapOutputValueClass(Text.class);
			jobB.setMapOutputKeyClass(IntWritable.class);
			jobB.setInputFormatClass(TextInputFormat.class);
			jobB.setMapperClass(Map2.class);
			jobB.setReducerClass(Reduce2.class);
			jobB.setOutputKeyClass(IntWritable.class);
			jobB.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(jobB, new Path(otherargs[1]));
			FileOutputFormat.setOutputPath(jobB, new Path(otherargs[2]));
			System.exit(jobB.waitForCompletion(true) ? 0 : 1);
		}
	}
}
