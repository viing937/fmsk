package cn.edu.seu.cloud.jn2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

public class Main {
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		String traindata = args[0];
		String testdata = args[1];
		String outputdir = args[2];
		String tempdir = "ving/temp/";
		int numIterations = 15;
		int numFeatures = 20;
		double lambda = 0.05;
		
		Configuration conf = new Configuration();
		conf.set("train_data", traindata);
		conf.set("test_data", testdata);
		conf.set("output_dir", outputdir);
		conf.set("tempdir", tempdir);
		conf.set("numIterations", String.valueOf(numIterations));
		conf.set("numFeatures", String.valueOf(numFeatures));
		conf.set("lambda", String.valueOf(lambda));

		Preprocessing.run(conf, traindata, tempdir+"preprocessing");
		CreateURating.run(conf, tempdir+"preprocessing/cleandata", tempdir+"userRating");
		CreateIRating.run(conf, tempdir+"preprocessing/cleandata", tempdir+"itemRating");
		InitM.run(conf, tempdir+"itemRating", tempdir+"M-0");
		for (int i = 1; i <= numIterations; i++) {
			conf.set("IterationPath", tempdir+"M-"+(i-1)+"/part-r-00000");
			Iteration.run(conf, tempdir+"userRating", tempdir+"U-"+i);
			conf.set("IterationPath", tempdir+"U-"+i+"/part-r-00000");
			Iteration.run(conf, tempdir+"itemRating", tempdir+"M-"+i);
		}

		//CalRMSE.run(conf, testdata, tempdir+"RMSE");
		Predict.run(conf, testdata, outputdir);
	}
}