package com.bfd.userprofile.latentdemand.mr;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.bfd.userprofile.latentdemand.writable.LabelPointWritable;

public class LatentTransformReducer extends Reducer<Text, LabelPointWritable, Text, Text> {
	private Text reducer_out_key = new Text();
	private Text reducer_out_value = new Text();

	private Map<String, Integer> cate_mapping = new HashMap<String, Integer>();

	@Override
	protected void reduce(Text key, Iterable<LabelPointWritable> values, Context context)
			throws IOException, InterruptedException {
		Set<String> feature_set = new HashSet<String>();
		Set<String> label_set = new HashSet<String>();
		
		for (LabelPointWritable labelPointWritable : values) {
			System.out.println("------"+key.toString()+"------"+labelPointWritable.getIndex()+" "+ labelPointWritable.getValue()+" "+ labelPointWritable.getType());
			if (labelPointWritable.getType().get() == 1) {
				LabelPointWritable copy1 = new LabelPointWritable(labelPointWritable.getIndex(), labelPointWritable.getValue(), labelPointWritable.getType());
//				System.out.println("======"+copy1.getIndex()+" "+ copy1.getValue()+" "+ copy1.getType());
				feature_set.add(String.format("%s:%s", copy1.getIndex().get(),copy1.getValue().get()));
			} else {
				
				LabelPointWritable copy2 = new LabelPointWritable(labelPointWritable.getIndex(), labelPointWritable.getValue(), labelPointWritable.getType());
//				System.out.println("======"+copy2.getIndex()+" "+ copy2.getValue()+" "+ copy2.getType());
				label_set.add(String.format("%s:%s", copy2.getIndex().get(),copy2.getValue().get()));
			}
		}
		
//		System.out.println(">>>>>>>>feature_list:"+feature_list.size());
		if(feature_set.size() > 0){
			
			StringBuffer feature_sb = new StringBuffer();
			for (String feature : feature_set) {
//				System.out.println("++*"+key.toString()+"*++"+feature);
				feature_sb.append(" ").append(feature);
//				System.out.println("------"+key.toString()+"----"+feature_copy.getIndex().get()+":"+feature_copy.getValue().get());
			}
			String feature_str = feature_sb.substring(1);

			if (!feature_str.isEmpty() && feature_str.length() != 0 && feature_str != null) {
				for (String cate_name : cate_mapping.keySet()) {
					int cate_index = cate_mapping.get(cate_name);
					boolean exit = false;
					for (String label : label_set) {
						if (Integer.parseInt(label.split(":")[0]) == cate_index) {
							exit = true;
							break;
						}
					}

					if (exit) {
//						System.out.println("****exit***"+cate_name);
						reducer_out_key.set(String.valueOf(cate_index));
						reducer_out_value.set(String.format("%s,%s", 1, feature_str));
					} else {
						reducer_out_key.set(String.valueOf(cate_index));
						reducer_out_value.set(String.format("%s,%s", 0, feature_str));
					}
					context.write(reducer_out_key, reducer_out_value);
				}
			}
		}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// step up the mapping of the category
		// the date : featrue date and label date
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
	}

	@Override
	protected void cleanup(Reducer<Text, LabelPointWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// DEBUG
		for (String cate_name : cate_mapping.keySet()) {
			System.out.println("===" + cate_name + " " + cate_mapping.get(cate_name) + "===");
		}

		cate_mapping.clear();

	}
}
