package hw1;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import hw1.Q5.Map5.Reduce5;

public class Q5 {
	public static class Map5 extends Mapper<LongWritable, Text, Text, LongWritable> {
		int count = 0;

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			count++;
			String[] lines = value.toString().split("\n");// separate usedata.txt into lines
			for (String line : lines) {
//				String[] words = line.split("\\s+");
				String[] words = line.split(",", -1);
				for (String word : words) {
					if (word.length() == 0)
						continue;
					context.write(new Text(word), new LongWritable(count));
				}
			}
		}

		public static class Reduce5 extends Reducer<Text, LongWritable, Text, Text> {
			public void reduce(Text key, Iterable<LongWritable> values, Context context)
					throws IOException, InterruptedException {
				ArrayList<String> temp = new ArrayList<>();

				for (LongWritable value : values) {

					temp.add(value.toString());
				}
				temp.sort((a, b) -> a.compareTo(b));
				context.write(key, new Text("[" + StringUtils.join(",", temp) + "]"));
			}
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherargs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "Inverted Index");
		job.setJarByClass(Q5.class);
		job.setMapperClass(Map5.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setReducerClass(Reduce5.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		conf.set("userDataPath", otherargs[0].trim());
		FileInputFormat.addInputPath(job, new Path(otherargs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherargs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
