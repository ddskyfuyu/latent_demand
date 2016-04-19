package com.bfd.userprofile.market.acceptlevel.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProbabilityComputerMap extends Mapper<LongWritable, Text, Text, Text> {
	private static  final String HIVE_DEFAULT_SEPARATOR = "\t";
	private static  final String HIVE_DEFAULT_COLUMNS = "2";
	
	private String hive_separator = null;
	private int columns = 2 ;

	@Override
	protected void map(LongWritable offset, Text line,Context context)
			throws IOException, InterruptedException {
		Text out_key = new Text();
		Text out_value = new Text();
		String[] e = line.toString().split(hive_separator);
		if (e.length == columns) {
			out_key.set(e[0]);
			out_value.set(e[1]);
			System.out.println("*************"+e[0] + e[1]);
			context.write(out_key, out_value);
		}
	}

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		Configuration conf = new Configuration();
		hive_separator = conf.get("hive_separator",HIVE_DEFAULT_SEPARATOR);
		columns = Integer.parseInt(conf.get("columns", HIVE_DEFAULT_COLUMNS));
	}
}
