package up.latent.data.prepare;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * 潜在需求数据准备，map函数
 * @author BaoquanZhang 2016-01-29
 *
 */
public class DataPrepareMap extends Mapper<LongWritable, Text, Text, Text> {
	
	String allMonth = null;	//所有月份字符串列
	int comCateAmount = 0;	//电商总类目
	int mediaCateAmount = 0;//媒体总类目
	
	private Text outputKey = new Text();
	private Text outputValue = new Text();
	
	
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		allMonth = conf.get("allMonth");
		comCateAmount = Integer.valueOf(conf.get("comCateAmount"));
		mediaCateAmount = Integer.valueOf(conf.get("mediaCateAmount"));
	}



	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {		
		
		if (value != null && value.toString().trim().length() > 0) {
			String line = value.toString().trim();
			String[] lineArray = line.split("#");
			if(lineArray.length == 3){
				
				String gid = lineArray[0].trim();
				String categoryIndex = lineArray[1].trim();
				String date = lineArray[2].trim();
				
				int columnIndex = -1;
				
				//防止日期格式中的异常
				if(date != null && date.length() > 0){
					String[] dateArray = date.split("-");
					if(dateArray.length == 3){
						String specificMonth = dateArray[0].trim() + dateArray[1].trim();
						columnIndex = ColumnIndexAnalysis.getColunmIndex(specificMonth, categoryIndex, allMonth, comCateAmount, mediaCateAmount);
					}
				}
				
				if(columnIndex != -1){
					outputKey.set(gid);
					outputValue.set(String.valueOf(columnIndex));
					context.write(outputKey,outputValue);
				}
				
			}
			
		}
	}
}
