package com.hpsk.zhizuobiao.JavaBigData.mapreduce.mysql;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReadFromMysql {
	
	public static void main(String[] args) throws  Exception{
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		DBConfiguration.configureDB(
				conf, 
				"com.mysql.jdbc.Driver", 
				"jdbc:mysql://192.168.134.188:3306/test", 
				"root", 
				"123456");
		Job job = Job.getInstance(conf, "read");
		job.setJarByClass(ReadFromMysql.class);
		//设置输入
		job.setInputFormatClass(DBInputFormat.class);
		String[] fields = {"id","ename"};
		DBInputFormat.setInput(
				job, 
				DBReader.class, 
				"emp", 
				null, 
				"id", 
				fields
				);
		//设置map
		job.setMapperClass(ReadMap.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		//设置reduce
		job.setNumReduceTasks(0);
		//设置输出
		Path outputPath = new Path("/data/mysql/output");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputPath)){
			fs.delete(outputPath, true);
		}
		FileOutputFormat.setOutputPath(job, outputPath);
		//提交
		job.waitForCompletion(true);
	}
	
	public static class ReadMap extends Mapper<LongWritable, DBReader, IntWritable, Text>{
		@Override
		protected void map(LongWritable key, DBReader value,
				Mapper<LongWritable, DBReader, IntWritable, Text>.Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(new IntWritable(value.getId()), new Text(value.getName()));
		}
	}


}
