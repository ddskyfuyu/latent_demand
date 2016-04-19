package com.bfd.userprofile.marketfeatures.acceptdegree.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AggregateMap extends Mapper<LongWritable, Text, Text, Text> {
	private static  final String HIVE_DEFAULT_SEPARATOR = "\001";
	private static  final String HIVE_DEFAULT_COLUMNS = "0";
	
	private String hive_separator = null;
	private int columns = 0 ;
	@Override
	protected void map(LongWritable offset, Text line,Context context)
			throws IOException, InterruptedException {
		Text mapper_out_key = new Text();
		Text mapper_out_value = new Text();
		
		String[] elements = line.toString().split(hive_separator);
		if(elements.length == columns){
			mapper_out_key.set(elements[0]);
			mapper_out_value.set(elements[columns]);
			context.write(mapper_out_key, mapper_out_value);
		}
	}

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		hive_separator = conf.get("hive_separator", HIVE_DEFAULT_SEPARATOR);
		columns = Integer.parseInt(conf.get("columns", HIVE_DEFAULT_COLUMNS));
	}
	
	
}
