package up.latent.data.prepare;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DataPrepareReduce extends Reducer<Text, Text, Text, NullWritable> {
	
	Text outputKey = new Text();	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		Map<Integer, Integer> map = new TreeMap<Integer, Integer>();
		String gid = key.toString();
		for(Text text : values){
			int columnIndex = Integer.valueOf(text.toString());
			if(map.containsKey(columnIndex)){
				int amount = map.get(columnIndex) + 1;
				map.put(columnIndex, amount);
			}else{
				map.put(columnIndex, 1);
			}
		}
		StringBuffer output = new StringBuffer();
		output.append(gid + " ");
		for(Map.Entry<Integer, Integer> entry : map.entrySet()){
			String columnIndex = String.valueOf(entry.getKey());
			String amout = String.valueOf(entry.getValue());
			output.append(columnIndex + ":" + amout + " ");
		}
		outputKey.set(output.toString().trim());
		context.write(outputKey, NullWritable.get());		
	}
}
