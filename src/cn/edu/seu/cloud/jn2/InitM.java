package cn.edu.seu.cloud.jn2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class InitM {
	public static class InitM_Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		private Text k = new Text();
		private DoubleWritable v = new DoubleWritable();
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().replaceFirst("\t", ",").split(",");
			String itemID = tokens[0];
			double sum = 0;
			for (int i = 1; i < tokens.length; i++) {
				String[] temp = tokens[i].split(":");
				sum += Double.parseDouble(temp[1]);
			}
			k.set(itemID);
			v.set(sum/(tokens.length-1));
			context.write(k, v);
		}
	}
	public static class InitM_Reducer extends Reducer<Text, DoubleWritable, Text, Text> {
		private Text v = new Text();
		protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			int numFeatures = Integer.parseInt(context.getConfiguration().get("numFeatures"));
			StringBuilder sb = new StringBuilder();
			for ( DoubleWritable val: values ) {
				sb.append(val.toString());
			}
			for (int i = 0; i < numFeatures-1; i++) {
				sb.append("," + Math.random());
			}
			v.set(sb.toString());
			context.write(key, v);
		}
	}
	public static void run( Configuration conf, String input, String output ) throws IOException, ClassNotFoundException, InterruptedException {
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "InitM");
		
		job.setNumReduceTasks(1);
		job.setJarByClass(InitM.class);
		
		job.setMapperClass(InitM_Mapper.class);
		job.setReducerClass(InitM_Reducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.waitForCompletion(true);
	}
}