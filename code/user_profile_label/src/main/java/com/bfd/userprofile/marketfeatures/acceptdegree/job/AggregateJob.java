package com.bfd.userprofile.marketfeatures.acceptdegree.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.bfd.userprofile.marketfeatures.acceptdegree.mr.AggregateMap;
import com.bfd.userprofile.marketfeatures.acceptdegree.mr.AggregateReduce;

public class AggregateJob {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("hive_separator", "\001");
		conf.set("columns", "3");
		
		Job job = new Job(conf);
		job.setJarByClass(AggregateJob.class);
		job.setJobName("AggregateJob");

		job.setMapperClass(AggregateMap.class);
		job.setCombinerClass(AggregateReduce.class);
		job.setReducerClass(AggregateReduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
