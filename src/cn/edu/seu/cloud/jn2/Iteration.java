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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Iteration {
	public static class Iteration_Mapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		private static Map<Integer,Vector<Double>> Uorm=new HashMap<Integer, Vector<Double>>();
		private LongWritable k = new LongWritable();
		private Text v = new Text();
		@SuppressWarnings("deprecation")
		protected void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			FileSystem hdfs = FileSystem.get(conf);
			
			FSDataInputStream dis = hdfs.open(new Path(conf.get("IterationPath")));
			String t = dis.readLine();
			while ( t != null ) {
				String[] tokens = t.replaceFirst("\t", ",").split(",");
				Vector<Double> temp = new Vector<Double>();
				for (int i=1; i < tokens.length; i++ ) {
					temp.add(Double.parseDouble(tokens[i]));
				}
				Uorm.put(Integer.parseInt(tokens[0]),temp);
				t = dis.readLine();
			}
		}
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//input
			String[] tokens = value.toString().replaceFirst("\t", ",").split(",");
			
			int numFeatures = Integer.parseInt(context.getConfiguration().get("numFeatures"));
			double lambda = Double.parseDouble((context.getConfiguration().get("lambda")));
			
			//featureVectors, MiIi, RiIiMaybeTransposed
			double[][] featureVectors = new double[tokens.length-1][numFeatures];
			double[][] MiIi = new double[numFeatures][tokens.length-1];
			double[] RiIiMaybeTransposed = new double[tokens.length-1];
			for ( int i = 1; i < tokens.length; i++ ) {
				String[] temp = tokens[i].split(":");
				int id = Integer.parseInt(temp[0]);
				RiIiMaybeTransposed[i-1] = Double.parseDouble(temp[1]);
				for ( int j = 0; j < numFeatures; j++) {
					featureVectors[i-1][j] = Uorm.get(id).get(j);
					MiIi[j][i-1] = featureVectors[i-1][j];
				}
			}
			//compute Ai = MiIi * t(MiIi) + lambda * nui * E
			double[][] Ai = new double[numFeatures][numFeatures];
			for ( int i = 0; i < numFeatures; i++ )
				for ( int j = 0; j < numFeatures; j++ ) {
					Ai[i][j] = 0;
					for ( int k = 0; k < tokens.length-1; k++ )
						Ai[i][j] += MiIi[i][k]*featureVectors[k][j];
				}
			for ( int i = 0; i < numFeatures; i++ )
				Ai[i][i] += lambda*(tokens.length-1);
			
			//compute Vi = MiIi * t(R(i,Ii))
			double[] Vi = new double[numFeatures];
			for ( int i = 0; i < numFeatures; i++ ) {
				Vi[i] = 0;
				for ( int j = 0; j < tokens.length-1; j++ )
					Vi[i] += MiIi[i][j]*RiIiMaybeTransposed[j];
			}
			
			//QRDecomposition
			double[][] q = Ai;
			double[][] r = new double[numFeatures][numFeatures];
			
			for ( int i = 0; i < numFeatures; i++ ) {
				double[] qi = new double[numFeatures];
				for ( int j = 0; j < numFeatures; j++ )
					qi[j] = q[j][i];
				
				double alpha = 0;
				for ( int j = 0; j < numFeatures; j++ ) {
					alpha += qi[j]*qi[j];
				}
				alpha = Math.sqrt(alpha);
				
				for ( int j = 0; j < numFeatures; j++ ) {
					qi[j] /= alpha;
					q[j][i] /= alpha;
				}
				
				r[i][i] = alpha;
				
				for ( int j = i + 1; j < numFeatures; j++ ) {
					double[] qj = new double[numFeatures];
					for ( int k = 0; k < numFeatures; k++ )
						qj[k] = q[k][j];
					
					double norm = 0;
					for ( int k = 0; k < numFeatures; k++ ) {
						norm += qj[k]*qj[k];
					}
					norm = Math.sqrt(norm);
					
					double beta = 0;
					for ( int k = 0; k < numFeatures; k++ )
						beta += qi[k]*qj[k];
					
					r[i][j] = beta;
					
					for ( int k = 0; k < numFeatures; k++ ) {
						qj[k] -= beta*qi[k];
						q[k][j] = qj[k];
					}
				}
			}
			
			double[][] qt = new double[numFeatures][numFeatures];
			for ( int i = 0; i < numFeatures; i++ )
				for ( int j = 0; j < numFeatures; j++ )
					qt[i][j] = q[j][i];
			
			double[] y = new double[numFeatures];
			for ( int i = 0; i < numFeatures; i++ ) {
				y[i] = 0;
				for ( int j = 0; j < numFeatures; j++ )
					y[i] += qt[i][j]*Vi[j];
			}
			
			double[] x = new double[numFeatures];
			
			for (int i = numFeatures - 1; i >= 0; i--) {
				x[i] = y[i]/r[i][i];
				for ( int l = 0; l < numFeatures; l++ )
					y[l] -= r[l][i]*x[i];
			}

			
			//output
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < numFeatures; i++ )
				sb.append(","+x[i]);
			
			k.set(Long.parseLong(tokens[0]));
			v.set(sb.toString().replaceFirst(",", ""));
			context.write(k, v);
		}
	}
	public static void run( Configuration conf, String input, String output ) throws IOException, ClassNotFoundException, InterruptedException {
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Iteration");
		
		job.setNumReduceTasks(1);
		
		job.setJarByClass(Iteration.class);
		
		job.setMapperClass(Iteration_Mapper.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.waitForCompletion(true);
	}
}