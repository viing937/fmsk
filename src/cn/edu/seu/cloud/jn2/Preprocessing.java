package cn.edu.seu.cloud.jn2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Preprocessing {
	public static class Preprocessing_Mapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text k = new Text();
		private Text v = new Text();
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split("::");
			k.set(tokens[0]);
			v.set(tokens[1]+","+tokens[2]);
			context.write(k, v);
		}
	}
	public static class Preprocessing_Reducer extends Reducer<Text, Text, NullWritable, Text> {
		private NullWritable k = null;
		private Text v = new Text();
		private MultipleOutputs<NullWritable,Text> mos;
		protected void setup(Context context) {
			mos = new MultipleOutputs<NullWritable, Text>(context);
		}
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Map<String, Integer> count = new HashMap<String, Integer>();
			List<String> li = new ArrayList<String>();
			for ( Text value: values ) {
				li.add(value.toString());
				String score = value.toString().split(",")[1];
				if ( count.containsKey(score) ) {
					count.put(score, count.get(score)+1);
				}
				else {
					count.put(score, 1);
				}
			}
			int max = 0;
			int times = 0;
			double sum_score = 0;
			for ( String score: count.keySet() ) {
				int cnt = count.get(score);
				times += cnt;
				sum_score += Double.parseDouble(score)*cnt;
				if ( cnt > max )
					max = cnt;
			}
			if ( 1.0*max/times < 0.15 || 1.0*max/times > 0.95  )
			{
				v.set(key.toString()+'\t'+sum_score/times);
				mos.write("noise", k, v, "noise/");
			}
			else
			{
				for ( String value: li ) {
					v.set(key.toString()+","+value);
					mos.write("cleandata", k, v, "cleandata/");
				}
			}
		}
		protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
	}
	public static void run( Configuration conf, String input, String output ) throws IOException, ClassNotFoundException, InterruptedException {
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Preprocessing");
		
		job.setNumReduceTasks(1);
		job.setJarByClass(Preprocessing.class);
		
		job.setMapperClass(Preprocessing_Mapper.class);
		job.setReducerClass(Preprocessing_Reducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		MultipleOutputs.addNamedOutput(job, "cleandata", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "noise", TextOutputFormat.class, NullWritable.class, Text.class);
		
		job.waitForCompletion(true);
	}
}