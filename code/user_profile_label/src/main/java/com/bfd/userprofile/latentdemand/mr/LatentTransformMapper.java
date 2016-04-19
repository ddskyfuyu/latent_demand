package com.bfd.userprofile.latentdemand.mr;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.bfd.userprofile.latentdemand.writable.LabelPointWritable;

public class LatentTransformMapper extends Mapper<LongWritable, Text, Text, LabelPointWritable> {
	
	private static Pattern p = Pattern.compile("^((\\d{2}(([02468][048])|([13579][26]))[\\-\\-\\s]?((((0?" +"[13578])|(1[02]))[\\-\\-\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))" +"|(((0?[469])|(11))[\\-\\-\\s]?((0?[1-9])|([1-2][0-9])|(30)))|" +"(0?2[\\-\\-\\s]?((0?[1-9])|([1-2][0-9])))))|(\\d{2}(([02468][12" +"35679])|([13579][01345789]))[\\-\\-\\s]?((((0?[13578])|(1[02]))" +"[\\-\\-\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))" +"[\\-\\-\\s]?((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\-\\s]?((0?[" +"1-9])|(1[0-9])|(2[0-8]))))))");
	
	private final String START_DATE = "START_DATE";
	private final String END_DATE = "END_DATE";
	private String hive_delimiter = "\001";

	private Text map_out_key = new Text();
	private LabelPointWritable lable_pair = new LabelPointWritable();

	private IntWritable index = new IntWritable();
	private IntWritable p_value = new IntWritable();
	private IntWritable type = new IntWritable();

	private Map<String, Integer> cate_mapping = new HashMap<String, Integer>();
	private Map<String, Integer> date_mapping = new HashMap<String, Integer>();
	private String start_date = "";
	private String end_date = "";

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] lines = value.toString().split(hive_delimiter);
		if (lines.length == 3) {
			String gid = lines[0];
			String cate = lines[1];
			String l_date = lines[2];

			if (gid.matches("^[0-9a-f]+$") && cate_mapping.containsKey(cate) && p.matcher(l_date).matches()) {

				map_out_key.set(gid);
				
				if (l_date.compareTo(start_date) >= 0 && l_date.compareTo(end_date) <= 0) {
//					System.out.println(">>>>>feature:"+l_date);
					int cate_index = (cate_mapping.get(cate) - 1) * date_mapping.size() + date_mapping.get(l_date);
					index.set(cate_index); // index need transform

					p_value.set(1);
					type.set(1);
				} else {
					index.set(cate_mapping.get(cate));
					p_value.set(1);
					type.set(2);
				}

				lable_pair.set(index, p_value, type);
				
//				System.out.println("&&&&&&"+lable_pair.getIndex()+":"+lable_pair.getValue()+":"+lable_pair.getType());
				context.write(map_out_key, lable_pair);
			}
		}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		start_date = conf.get(START_DATE,"2015-05-01");
		end_date = conf.get(END_DATE,"2015-05-25");

		BufferedReader attr_reader = null;
		attr_reader = new BufferedReader(new InputStreamReader(new FileInputStream("cate_mappiing_file")));

		String line1 = "";
		while (((line1 = attr_reader.readLine()) != null)) {
			if (line1.trim().isEmpty() == true) {
				continue;
			}
			String[] attrs = line1.trim().split(",");
			cate_mapping.put(attrs[0], Integer.parseInt(attrs[1]));
		}
		attr_reader.close();
		
		attr_reader = new BufferedReader(new InputStreamReader(new FileInputStream("date_mappiing_file")));
		String line2 = "";
		while (((line2 = attr_reader.readLine()) != null)) {
			if (line2.trim().isEmpty() == true) {
				continue;
			}
			String[] attrs = line2.trim().split(",");
			date_mapping.put(attrs[0], Integer.parseInt(attrs[1]));
		}
		attr_reader.close();
	}

	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, LabelPointWritable>.Context context)
			throws IOException, InterruptedException {
		// DEBUG
		for (String cate_name : cate_mapping.keySet()) {
			System.out.println("===" + cate_name + " " + cate_mapping.get(cate_name) + "===");
		}
		for (String date_name : date_mapping.keySet()) {
			System.out.println("===" + date_name + " " + date_mapping.get(date_name) + "===");
		}

		cate_mapping.clear();
		date_mapping.clear();
	}

}
