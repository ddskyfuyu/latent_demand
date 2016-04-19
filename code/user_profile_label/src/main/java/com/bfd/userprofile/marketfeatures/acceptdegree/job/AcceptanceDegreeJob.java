package com.bfd.userprofile.marketfeatures.acceptdegree.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bfd.userprofile.marketfeatures.acceptdegree.mr.AggregateMap;
import com.bfd.userprofile.marketfeatures.acceptdegree.mr.AggregateReduce;
import com.bfd.userprofile.marketfeatures.acceptdegree.mr.ProbabilityComputerMap;
import com.bfd.userprofile.marketfeatures.acceptdegree.mr.ProbabilityComputerReducer;

public class AcceptanceDegreeJob extends Configured implements Tool{

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String levels = "1=0.001,2=0.002,3=0.003,4=0.004,5=0.005,6=0.006,7=0.007,8=0.008,9=0.009,10=0.010,11=0.011,12=0.012,13=0.013,14=0.014,15=0.015,16=0.016,17=0.017,18=0.018,19=0.019,20=0.020,21=0.021,22=0.022,23=0.023,24=0.024,25=0.025,26=0.026,27=0.027,28=0.028,29=0.029,30=0.030,31=0.031,32=0.032,33=0.033,34=0.034,35=0.035,36=0.036,37=0.037,38=0.038,39=0.039,40=0.040,41=0.041,42=0.042,43=0.043,44=0.044,45=0.045,46=0.046,47=0.047,48=0.048,49=0.049,50=0.050,51=0.051,52=0.052,53=0.053,54=0.054,55=0.055,56=0.056,57=0.057,58=0.058,59=0.059,60=0.060,61=0.061,62=0.062,63=0.063,64=0.064,65=0.065,66=0.066,67=0.067,68=0.068,69=0.069,70=0.070,71=0.071,72=0.072,73=0.073,74=0.074,75=0.075,76=0.076,77=0.077,78=0.078,79=0.079,80=0.080,81=0.081,82=0.082,83=0.083,84=0.084,85=0.085,86=0.086,87=0.087,88=0.088,89=0.089,90=0.090,91=0.091,92=0.092,93=0.093,94=0.094,95=0.095,96=0.096,97=0.097,98=0.098,99=0.099,100=0.1";
//		conf.set("hive_separator", "\t");
		conf.set("columns", "2");
		conf.set("max_level", "100");
		conf.set("prod_list", levels);
		
		Job job = new Job(conf);
		job.setJarByClass(AcceptanceDegreeJob.class);
		job.setJobName("AcceptanceDegreeJob");

		job.setMapperClass(ProbabilityComputerMap.class);
//		job.setCombinerClass(ProbabilityComputerReducer.class);
		job.setReducerClass(ProbabilityComputerReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
//		String input_file_1 ="/user/bre/tiefan.ding/market_tag_degree_src/dt=2015-06-23";
//		String input_file_2 ="/user/bre/tiefan.ding/market_tag_degree_prob/dt=2015-06-23";
//		String output_file ="/user/bre/tiefan.ding/degree_test";
		
		MultipleInputs.addInputPath(job, new Path(args[0]),
				TextInputFormat.class,ProbabilityComputerMap.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),
				TextInputFormat.class,ProbabilityComputerMap.class);
		
		
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.waitForCompletion(true);
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new AcceptanceDegreeJob(), args);
	}
}
