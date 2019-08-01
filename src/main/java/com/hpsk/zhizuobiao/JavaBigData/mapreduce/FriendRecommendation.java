package com.hpsk.zhizuobiao.JavaBigData.mapreduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FriendRecommendation extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		/**
		 * 创建一个mapreduce的job
		 */
		Job job = Job.getInstance(this.getConf(), "friend");
		job.setJarByClass(FriendRecommendation.class);
		/**
		 * 配置job
		 */
		//配置读取数据的路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		//配置map
		job.setMapperClass(FriendMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//配置shuffle--自动
		//配置reduce
		job.setReducerClass(FriendReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//配置output
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		/**
		 * 提交job
		 */
		return job.waitForCompletion(true) ? 0:-1;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		//调用自己这个类的run方法
		try {
			int status = ToolRunner.run(conf, new FriendRecommendation(), args);
			System.exit(status);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static class FriendMap extends Mapper<LongWritable, Text, Text, Text>{
		
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			String[] users = line.split("\t");
			String user = users[0];
			String friend = users[1];
			context.write(new Text(user), new  Text(friend));
			context.write(new  Text(friend), new Text(user));
		}
	}
	
	public static class FriendReduce extends Reducer<Text, Text, Text, Text>{
		
		private Set<String> set = new HashSet<String>();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			set.clear();
			for(Text text : values){
				set.add(text.toString());
			}
			if(set.size() > 1){
				for(Iterator<String> i = set.iterator();i.hasNext();){
					String name = (String)i.next();
					for(Iterator<String> j = set.iterator();j.hasNext();){
						String other = (String)j.next();
						if(!name.equals(other)){
							context.write(new Text(name), new Text(other));
						}
					}
				}
			}
			
		}
	}
}
