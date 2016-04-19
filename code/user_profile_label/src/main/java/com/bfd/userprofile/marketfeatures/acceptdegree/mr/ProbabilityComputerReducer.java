package com.bfd.userprofile.marketfeatures.acceptdegree.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProbabilityComputerReducer extends Reducer<Text, Text, Text, Text> {
	// 先实现功能，此处应该是需要去区别数据的来源的（左表还是右表），需要一种更好，更优雅的方式来实现。
	// **** 现在就利用数据的类型不同来区别
	private int max_level = 100;
	private Map<Integer,Float> levels = null;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		max_level = Integer.parseInt(conf.get("max_level", "1"));
		levels = getLevels(conf);
	}

	private Map<Integer, Float> getLevels(Configuration conf) {
		Map<Integer,Float> levels_tmp = new HashMap<Integer,Float>();
		String prod_list = conf.get("prod_list");
		for (String mapping : prod_list.split(",")) {
			String[] level_value = mapping.split("=");
			levels_tmp.put(Integer.parseInt(level_value[0]), Float.parseFloat(level_value[1]));
		}
		System.out.println("*************"+levels_tmp.toString());
		return levels_tmp;
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		List<String> clicks = new ArrayList<String>();
		Map<Integer,Float> level_value_mapping = new HashMap<Integer, Float>();
		
		for (Text value : values) {
			System.out.println("********reduce*****"+value.toString());
			String valueStr = value.toString();
			if (valueStr.contains(".") && valueStr.contains("=")) {//此处暂时通过数据的类型来确定接受的数据是左表还是右表
				
				for (String str : valueStr.split(",")) {
					int level = Integer.parseInt(str.split("=")[0].trim());
					float level_value = Float.parseFloat(str.split("=")[1].trim());
					level_value_mapping.put(level, level_value);
				}
				
			} else {
				for (String str : valueStr.split(",")) {
					clicks.add(str);
				}
			}
		}
		
		computerpostprob(key, clicks, level_value_mapping, context);
	}

	private void computerpostprob(Text key, List<String> clicks, Map<Integer,Float> prod_list, Context context) throws IOException, InterruptedException {
		System.out.println("========clicks=="+clicks.toString());
		System.out.println("========prod_list=="+prod_list.toString());
		
		Text out_value = new Text();
		if (clicks.size() == 0) {
			//该用户没有被曝光，不做任何计算，直接输出原有的概率分布情况
			String vstr = mapToString(prod_list);
			out_value.set(vstr);
			context.write(key, out_value);
		} else {
			//该用户被曝光了，通过点击行为来更新概率分布。
			if (prod_list.size() < 1) {
				//新用户，没有历史概率分布。首先为其生成一个初始化概率分布
				prod_list = initprod();
			}
			
			Map<Integer,Float> new_prod_list = postprob(clicks,prod_list);
			
			String vstr = mapToString(new_prod_list);
			
//			String vstr = new_prod_list.toString();
//			out_value.set(vstr.substring(1,vstr.length()-1));
			out_value.set(vstr);
			context.write(key, out_value);
		}
	}

	private String mapToString(Map<Integer, Float> new_prod_list) {
		Set<Integer> keyset = new_prod_list.keySet();
		Iterator<Integer> ir = keyset.iterator();
		StringBuffer temps= new StringBuffer();
		String s=null;
		while(ir.hasNext()){
			int key = ir.next();
			float value = new_prod_list.get(key);
		    temps.append(String.format("%d=%f", key,value)).append(",");
		}
		s = new String(temps.substring(0,temps.length()-1));
		return s;
	}

	private Map<Integer,Float> initprod() {
		Map<Integer,Float> level_value_mapping = new HashMap<Integer, Float>();
		float init_level = 1.0F/max_level;
		for (int i = 1; i <= max_level; i++) {
			level_value_mapping.put(i, init_level);
		}
		return level_value_mapping;
	}

	private Map<Integer,Float> postprob(List<String> clicks, Map<Integer,Float> prod_list) {
		System.out.println("========clicks2=="+clicks.toString());
		System.out.println("========prod_list2=="+prod_list.toString());
		
		for (String click : clicks) {
			float sum = 0.0F;
			// 从新计算概率分布
			for (int index : prod_list.keySet()) {
				float new_prod = 0;
				if(click.equals("0")){
					//曝光未点击
					new_prod = (1-levels.get(index))*prod_list.get(index);
				}else{
					//曝光且点击
					new_prod = levels.get(index)*prod_list.get(index);
				}
				prod_list.put(index, new_prod);
				sum += new_prod;
			}
			
			//归一化
			for (int index : prod_list.keySet()) {
				float nomal_value = prod_list.get(index) / sum;
				prod_list.put(index, nomal_value);
			}
		}
		return prod_list;
	}
}
