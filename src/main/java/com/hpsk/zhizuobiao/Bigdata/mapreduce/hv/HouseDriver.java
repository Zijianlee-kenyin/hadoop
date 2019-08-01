package com.hpsk.zhizuobiao.Bigdata.mapreduce.hv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class HouseDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(this.getConf() , "esf-hv");
		job.setJarByClass(HouseDriver.class);
		Path inputPaths = new Path(args[0]);
		FileInputFormat.setInputPaths(job, inputPaths);
		Path outputDir = new Path(args[1]);
		//输出目录不能提前存在
		FileSystem fs = FileSystem.get(this.getConf());
		if(fs.exists(outputDir)) {
			//第二个参数表示递归，目录给true，文件给false
			fs.delete(outputDir, true);
		}
		FileOutputFormat.setOutputPath(job, outputDir);
		job.setMapperClass(HouseMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(HouseWritable.class);
		job.setReducerClass(HouseReduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(HouseWritable.class);
		return job.waitForCompletion(true)?0:-1;
	}
	
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		try {
			ToolRunner.run(conf, new HouseDriver(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
