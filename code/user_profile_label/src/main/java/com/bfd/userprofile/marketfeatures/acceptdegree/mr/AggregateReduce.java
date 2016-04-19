package com.bfd.userprofile.marketfeatures.acceptdegree.mr;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AggregateReduce extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		Text out_key = new Text();
		Text out_vale = new Text();
		
		StringBuffer sb = new StringBuffer();
		for (Text value : values) {
			sb.append(value.toString()).append(",");;
		}
		out_key.set(key);
		out_vale.set(sb.substring(0,sb.length()-1));
		
		context.write(out_key, out_vale);
	}
}
