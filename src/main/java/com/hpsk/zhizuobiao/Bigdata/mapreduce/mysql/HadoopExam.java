package com.hpsk.zhizuobiao.Bigdata.mapreduce.mysql;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class HadoopExam {
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		/**
		 * 请根据实际情况修改数据库的地址
		 */
		DBConfiguration.configureDB(
				conf, 
				"com.mysql.jdbc.Driver", 
				"jdbc:mysql://192.168.213.134:3306/test", 
				"root",
				"123456"
				);
		Job job = Job.getInstance(conf, "exam");
		job.setJarByClass(HadoopExam.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		job.setMapperClass(ExamMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(UserWritable.class);
		job.setReducerClass(ExamReduce.class);
		job.setOutputKeyClass(TaobaoWritable.class);
		job.setOutputValueClass(NullWritable.class);
		//将reduce的结果输出到MySQL中
		job.setOutputFormatClass(DBOutputFormat.class);
		String[] fields = {"province","usernumber","malenumber","femalenumber","ordernumber"};
		//设置job输出的地址，表名、以及列名
		DBOutputFormat.setOutput(job, "users", fields);
		job.waitForCompletion(true);
	}
	
public static class ExamMap extends Mapper<LongWritable, Text, Text, UserWritable>{
		
		private Text outputKey = new Text();
		private UserWritable outputValue = new  UserWritable();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			String[] items = line.split("\t");
			/**
			 * 填空1：请实现，将省份作为key，将用户id、性别、行为三个字段作为value输出
			 * 	注：自定义数据类型已定义好，直接赋值即可
			 */
			String province=items[8];
			String userId=items[1];
			String sex=items[3];
			String behavior=items[5];
			if(items.length>=9) {
				if(StringUtils.isNotBlank(province)&&StringUtils.isNotBlank(userId)
						&&StringUtils.isNotBlank(behavior)&&StringUtils.isNotBlank(sex)) {
					
						outputKey.set(province);
						outputValue.setAll(userId, Integer.valueOf(sex), Integer.valueOf(behavior));
						context.write(outputKey, outputValue);
					
					
				}
			}
		}
	}
	
	public static class ExamReduce extends Reducer<Text, UserWritable, TaobaoWritable, NullWritable>{
		
		private TaobaoWritable outputKey = new TaobaoWritable();
		private NullWritable outputValue = NullWritable.get();
		private Set<String> uidset = new HashSet<String>();
		private Set<String> maleset = new HashSet<String>();
		private Set<String> female = new HashSet<String>();
		private Set<String> order = new HashSet<String>();
		@Override
		protected void reduce(Text key, Iterable<UserWritable> values,
				Reducer<Text, UserWritable, TaobaoWritable, NullWritable>.Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//统计用户个数、男性个数、女性个数、订单个数
			/**
			 * 填空二：请实现对每个省份中的用户个数、男性个数、女性个数、订单个数进行统计，并封装到自定义的 Key中
			 * 		最终输出到MySQL中
			 */
			uidset.clear();
			maleset.clear();
			female.clear();
			order.clear();
			for(UserWritable val:values) {

				uidset.add(val.getUuid());
				if(val.getSex()==0) maleset.add(val.getUuid());
				if(val.getSex()==1) female.add(val.getUuid());
				if(val.getBehavior()==4) order.add(val.getUuid());
				
			}
			outputKey.setProvince(key.toString());
			outputKey.setUserNum(uidset.size());
			outputKey.setMaleNum(maleset.size());
			outputKey.setFemaleNum(female.size());
			outputKey.setOrderNum(order.size());
			context.write(outputKey, outputValue);
		}
	}

}
