package hw1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.HashMap;

public class Q4 {

	public static class Map1 extends Mapper<LongWritable, Text, LongWritable, Text> {

		private LongWritable user = new LongWritable();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("\t"); // id \t friends
			if (line.length == 2) {
				user.set(Long.parseLong(line[0]));
				context.write(user, new Text(line[1]));

			}
		}
	}

	public static class Reduce1 extends Reducer<LongWritable, Text, LongWritable, Text> {

		public HashMap<String, String> map = new HashMap<String, String>();

		int month = 0;
		int day = 0;
		int year = 0;

		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			map = new HashMap<String, String>();
			String userDataPath = conf.get("userDataPath");
			Path path = new Path("hdfs://localhost:9000" + userDataPath);
			FileSystem fs = FileSystem.get(conf);
			BufferedReader buff = new BufferedReader(new InputStreamReader(fs.open(path)));

			String input = buff.readLine();
			while (input != null) {
				String[] temp = input.split(",");
				if (temp.length == 10) {
					// get id and date of birth
					map.put(temp[0].trim(), temp[9].trim());
				}

				input = buff.readLine();
			}

			Calendar cal = Calendar.getInstance();
			month = cal.get(Calendar.MONTH);
			day = cal.get(Calendar.DAY_OF_MONTH);
			year = cal.get(Calendar.YEAR);
		}

		public void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String user = "";
			for (Text value : values) {

				user = value.toString();

			}

			int max = 0, result;
			if (!user.isEmpty() && user != null) {
				String[] friends = user.toString().split(",");
				for (String friend : friends) {
					if (map.containsKey(friend)) {
						String dob = map.get(friend);
						String[] y = dob.split("/");
						if (y.length == 3) {
							result = year - Integer.parseInt(y[2]);
							if (Integer.parseInt(y[0]) > month) {
								result--;
							} else if (Integer.parseInt(y[0]) == month) {
								if (Integer.parseInt(y[1]) > day) {
									result--;
								}
							}
							max = Math.max(max, result);

						}

					}
				}

				context.write(key, new Text(max + ""));

			}

		}

	}

	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();
		String[] otherargs = new GenericOptionsParser(conf, args).getRemainingArgs();

		conf.set("userDataPath", otherargs[0]); // userDataPath
		Job jobA = Job.getInstance(conf, "Getting max age of friends");
		jobA.setJarByClass(Q4.class);
		jobA.setReducerClass(Reduce1.class);

		MultipleInputs.addInputPath(jobA, new Path(otherargs[1]), TextInputFormat.class, Map1.class);

		jobA.setOutputKeyClass(LongWritable.class);
		jobA.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(jobA, new Path(otherargs[2]));
		System.exit(jobA.waitForCompletion(true) ? 0 : 1);

	}
}
