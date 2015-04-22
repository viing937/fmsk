package cn.edu.seu.cloud.jn2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CreateURating {
	public static class CreateURating_Mapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text k = new Text();
		private Text v = new Text();
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			String userID = tokens[0];
			String itemID = tokens[1];
			String score = tokens[2];
			k.set(userID);
			v.set(itemID+":"+score);
			context.write(k, v);
		}
	}
	public static class CreateURating_Reducer extends Reducer<Text, Text, Text, Text> {
		private Text v = new Text();
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			for ( Text val: values ) {
				sb.append(","+val.toString());
			}
			v.set(sb.toString().replaceFirst(",", ""));
			context.write(key, v);
		}
	}
	public static void run( Configuration conf, String input, String output ) throws IOException, ClassNotFoundException, InterruptedException {
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "CreateUserRating");
		
		job.setJarByClass(CreateURating.class);
		
		job.setMapperClass(CreateURating_Mapper.class);
		job.setReducerClass(CreateURating_Reducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.waitForCompletion(true);
	}
}
