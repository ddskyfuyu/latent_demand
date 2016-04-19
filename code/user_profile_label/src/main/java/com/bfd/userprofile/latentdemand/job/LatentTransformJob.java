package com.bfd.userprofile.latentdemand.job;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bfd.userprofile.latentdemand.mr.LatentTransformMapper;
import com.bfd.userprofile.latentdemand.mr.LatentTransformReducer;
import com.bfd.userprofile.latentdemand.writable.LabelPointWritable;

public class LatentTransformJob extends Configured implements Tool{
	private final String START_DATE = "START_DATE";
	private final String END_DATE = "END_DATE";

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 6) {
			System.out.println("The number of parameters is wrong.");
			return 0;
		}
		
		conf.set(START_DATE, args[0]);
		conf.set(END_DATE, args[1]);
		conf.set("mapred.textoutputformat.separator", ",");
		
		Job job = Job.getInstance(conf,"LatentTransformJob");
		
		job.setJarByClass(LatentTransformJob.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(LatentTransformMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LabelPointWritable.class);
		job.setReducerClass(LatentTransformReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		job.addCacheFile(new URI(args[2]+"#cate_mappiing_file"));
		job.addCacheFile(new URI(args[3]+"#date_mappiing_file"));
		
		FileInputFormat.addInputPath(job, new Path(args[4]));
		FileOutputFormat.setOutputPath(job, new Path(args[5]));
		job.setNumReduceTasks(Integer.parseInt(args[6]));
		
		//DEBUG
		for (int i = 0; i < otherArgs.length; i++) {
			System.out.println("The " + i + " argments is " + otherArgs[i]);
		}
		
		return job.waitForCompletion(true)? 1 : 0;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new LatentTransformJob(), args));
	}

}
