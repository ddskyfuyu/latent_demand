package up.latent.data.prepare;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class DataPrepareMR extends Configured implements Tool {
	
	Logger LOG = Logger.getLogger(DataPrepareMR.class);	
	
	public int run(String[] args) throws Exception {
		if(args.length < 7){
			LOG.error("use 7 param, the current param is " + args.length );
			return 1;
		}

		for(int i = 0; i < args.length; i++){
			LOG.info(i + " : " + args[i]);
		}
		
		String comInputDir = args[0].trim();	//电商输入目录
		String mediaInputDir = args[1].trim();	//媒体输入目录
		String outputDir = args[2].trim();		//输出目录
		String allMonth=args[3].trim();			//所有月份列表
		String comCateAmount = args[4].trim();	//电商总品类数
		String mediaCateAmount = args[5].trim();//媒体总品类数
		String reduceNum = args[6].trim();
		
		Configuration conf = getConf();
		conf.set("allMonth", allMonth);
		conf.set("comCateAmount", comCateAmount);
		conf.set("mediaCateAmount", mediaCateAmount);
		
		FileSystem fs = FileSystem.get(conf);
		Path hdfsOutputDir = new Path(outputDir);
		if(fs.exists(hdfsOutputDir)){
			boolean outputDirIsDelete = fs.delete(hdfsOutputDir, true);
			if(outputDirIsDelete){
				LOG.info("删除目录 " + outputDir + " 成功！");
			}else {
				LOG.info("删除目录 " + outputDir + "失败！");
			}
		}
		
		Job job = Job.getInstance(conf);
		job.setJobName("latentDataPreparation");
		job.setJarByClass(DataPrepareMR.class);
		
		job.setMapperClass(DataPrepareMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(DataPrepareReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(Integer.valueOf(reduceNum));
		
		Path comPath = new Path(comInputDir);
		Path mediaPath = new Path(mediaInputDir);
		FileInputFormat.addInputPath(job, comPath);
		FileInputFormat.addInputPath(job, mediaPath);
		
		FileOutputFormat.setOutputPath(job, hdfsOutputDir);

		return job.waitForCompletion(true) ? 0:1;
	}




	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new DataPrepareMR(), args);
		System.exit(result);
	}

	
}
