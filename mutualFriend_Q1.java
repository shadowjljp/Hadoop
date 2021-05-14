package hw1;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

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

import hw1.mutualFriend_Q1.Map.Reduce;

public class mutualFriend_Q1 {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		Set<String> set;
		String pair = "";

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { // get
																													// the
																													// name
																													// list
//			Configuration conf = context.getConfiguration();
//			String otherargs = conf.get("nameList");
			String otherargs = "(0,1), (20, 28193), (1, 29826), (6222, 19272), (28041, 28056)";
			otherargs = otherargs.replaceAll("[\\[\\](){ }]", "");
			String[] nameList = otherargs.split(",");
			set = new HashSet<>();
			for (int i = 0; i < nameList.length; i += 2) {
				String s1 = nameList[i];
				String s2 = nameList[i + 1];
				String res = s1 + "," + s2;
				set.add(res);
			}

			String[] line = value.toString().split("\t");
			if (line.length == 2) {
				int AId = Integer.parseInt(line[0]);
				String[] friends = line[1].replaceAll("\\s", "").split(",");
				for (String friend : friends) {

					int BId = Integer.parseInt(friend);
					if (AId <= BId) {
						pair = AId + "," + BId;
					} else {
						pair = BId + "," + AId;
					}
					if (set.contains(pair)) {
						context.write(new Text(pair), new Text(line[1]));
					}

				}
			}
		}

		public static class Reduce extends Reducer<Text, Text, Text, Text> {
			private Text result = new Text();

			public void reduce(Text key, Iterable<Text> values, Context context)
					throws IOException, InterruptedException {
				HashMap<String, Boolean> map = new HashMap<>();
				StringBuilder mutualFriends = new StringBuilder();
				for (Text value : values) {
					String[] friends = value.toString().split(",");
					for (String fr : friends) {
						if (map.containsKey(fr)) {
							mutualFriends.append(fr + ",");
						} else {
							map.put(fr, true);
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

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherargs = new GenericOptionsParser(conf, args).getRemainingArgs();
//		conf.set("nameList", otherargs[2]);
		Job job = Job.getInstance(conf, "Mutual Friends");
		job.setJarByClass(mutualFriend_Q1.class);

		job.setReducerClass(Reduce.class);
		job.setMapperClass(Map.class);
		job.setOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherargs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherargs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
