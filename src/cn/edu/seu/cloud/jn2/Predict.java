package cn.edu.seu.cloud.jn2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Predict {
	public static class Predict_Mapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		private static Map<Integer,Vector<Double>> U=new HashMap<Integer, Vector<Double>>();
		private static Map<Integer,Vector<Double>> M=new HashMap<Integer, Vector<Double>>();
		private static Map<Integer, Double> noise = new HashMap<Integer, Double>();
		private LongWritable k = new LongWritable();
		private Text v = new Text();
		
		@SuppressWarnings("deprecation")
		protected void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			String tempdir = conf.get("tempdir");
			String numIterations = conf.get("numIterations");
			FileSystem hdfs = FileSystem.get(conf);
			
			FSDataInputStream dis1 = hdfs.open(new Path(tempdir+"M-"+numIterations+"/part-r-00000"));
			String t1 = dis1.readLine();
			while ( t1 != null ) {
				String[] tokens = t1.replaceFirst("\t", ",").split(",");
				Vector<Double> temp = new Vector<Double>();
				for (int i=1; i < tokens.length; i++ ) {
					temp.add(Double.parseDouble(tokens[i]));
				}
				M.put(Integer.parseInt(tokens[0]),temp);
				t1 = dis1.readLine();
			}
			
			FSDataInputStream dis2 = hdfs.open(new Path(tempdir+"U-"+numIterations+"/part-r-00000"));
			String t2 = dis2.readLine();
			while ( t2 != null ) {
				String[] tokens = t2.replaceFirst("\t", ",").split(",");
				Vector<Double> temp = new Vector<Double>();
				for (int i=1; i < tokens.length; i++ ) {
					temp.add(Double.parseDouble(tokens[i]));
				}
				U.put(Integer.parseInt(tokens[0]),temp);
				t2 = dis2.readLine();
			}
			
			if ( hdfs.exists(new Path(tempdir+"preprocessing/noise/-r-00000"))) {
				FSDataInputStream dis3 = hdfs.open(new Path(tempdir+"preprocessing/noise/-r-00000"));
				String t3 = dis3.readLine();
				while ( t3 != null ) {
					String[] tokens = t3.split("\t");
					noise.put(Integer.parseInt(tokens[0]), Double.parseDouble(tokens[1]));
					t3 = dis3.readLine();
				}
			}
		}
		
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split("::");
			int userID = Integer.parseInt(tokens[0]);
			int itemID = Integer.parseInt(tokens[1]);
			double score = Double.parseDouble(tokens[2]);
			
			Vector<Double> userFeatures = U.get(userID);
			Vector<Double> itemFeatures = M.get(itemID);
			
			double ans = 0;
			if (noise.containsKey(userID))
			{
				ans = noise.get(userID);
			}
			else if (userFeatures != null && itemFeatures != null) {
				for(int i = 0; i < userFeatures.size(); i++ ) {
					ans += userFeatures.get(i)*itemFeatures.get(i);
				}
				if ( ans > 5 )
					ans = 5;
				else if ( ans < 0.5 )
					ans = 0.5;
			} else ans = 3;
			k.set(1);
			v.set(String.valueOf(userID)+":"+String.valueOf(itemID)+":"+(ans-score)*(ans-score));
			context.write(k, v);
		}
	}
	public static class Predict_Reducer extends Reducer<LongWritable, Text, NullWritable, Text> {
		private NullWritable k = null;
		protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for ( Text val: values ) {
				context.write(k, val);
			}
		}
	}
	public static void run(Configuration conf, String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Predict");
		
		job.setJarByClass(Predict.class);
		
		job.setMapperClass(Predict_Mapper.class);
		job.setReducerClass(Predict_Reducer.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.waitForCompletion(true);
	}
}