package hw1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import java.util.HashMap;

public class Q3 {

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

		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Boolean> map = new HashMap<>();
			StringBuilder mutualFriends = new StringBuilder();
			for (Text value : values) {
				String[] friends = value.toString().split(",");
				for (String friend : friends) {
					if (map.containsKey(friend)) {
						mutualFriends.append(friend + ',');
					} else {
						map.put(friend, true);
					}
				}
			}

			if (mutualFriends.lastIndexOf(",") > -1) {

				mutualFriends.deleteCharAt(mutualFriends.lastIndexOf(","));
			}
			result.set(new Text(mutualFriends.toString()));
			context.write(key, result);
		}

	}

	public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
		private static HashMap<String, String> users;
		String AId = "";
		String BId = "";

		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			users = new HashMap<String, String>();
			String userDataPath = conf.get("userDataPath");
			Path path = new Path("hdfs://localhost:9000" + userDataPath);
			FileSystem fs = FileSystem.get(conf);
			BufferedReader buff = new BufferedReader(new InputStreamReader(fs.open(path)));

			String input = buff.readLine();
			while (input != null) {
				String[] temp = input.split(",");
				// every line in userdata.txt length should be 10
				if (temp.length == 10) {
					// users = {key : name+target value}
					String user = temp[1] + " " + temp[2] + ": " + temp[9];
					users.put(temp[0].trim(), user);
				}

				input = buff.readLine();
			}
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// value is key pair \t mutual friend list
			String[] line = value.toString().split("\t");
			if (line.length != 2) {
				return;
			}
			Configuration conf = context.getConfiguration();
			AId = conf.get("AId");
			BId = conf.get("BId");
			// line[0] is key pair.length==2
			String[] parts = line[0].split(",");
			if (parts[0] != null && parts[1] != null) {
				// part is Aid and Bid, sequence is random
				if ((parts[0].equals(AId) && parts[1].equals(BId)) || (parts[1].equals(AId) && parts[0].equals(BId))) {

					// line[1] is mutual friend list
					String[] mutualFriends = line[1].split(",");
					for (String mutualFriend : mutualFriends) {
						if (users.containsKey(mutualFriend)) {
							String details = users.get(mutualFriend);
							// line[0] is key pair.length==2 detail is the name+target value
							context.write(new Text(line[0]), new Text(details));
						}

					}
				}
			}

		}
	}

	public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			ArrayList<String> temp = new ArrayList<String>();

			for (Text value : values) {

				temp.add(value.toString());
			}

			context.write(key, new Text("[" + StringUtils.join(",", temp) + "]"));
		}

	}

	public static void main(String args[]) throws Exception {
		Configuration confA = new Configuration();
		String[] otherargs = new GenericOptionsParser(confA, args).getRemainingArgs();

		Job jobA = Job.getInstance(confA, "MutualFriends");
		jobA.setJarByClass(Q3.class);

		jobA.setMapperClass(Map1.class);
		jobA.setReducerClass(Reduce1.class);
		jobA.setOutputKeyClass(Text.class);
		jobA.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(jobA, new Path(otherargs[1]));
		FileOutputFormat.setOutputPath(jobA, new Path(otherargs[2]));
		boolean mapReduce = jobA.waitForCompletion(true);

		if (mapReduce) {
			Configuration confB = new Configuration();
			confB.set("AId", otherargs[4].trim());
			confB.set("BId", otherargs[5].trim());
			confB.set("userDataPath", otherargs[0].trim());
			Job jobB = Job.getInstance(confB, "MutualFriends");
			jobB.setJarByClass(Q3.class);

			jobB.setMapperClass(Map2.class);
			jobB.setReducerClass(Reduce2.class);
			jobB.setInputFormatClass(TextInputFormat.class);

			jobB.setOutputKeyClass(Text.class);
			jobB.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(jobB, new Path(otherargs[2]));
			FileOutputFormat.setOutputPath(jobB, new Path(otherargs[3]));

			System.exit(jobB.waitForCompletion(true) ? 0 : 1);
		}

	}
}
