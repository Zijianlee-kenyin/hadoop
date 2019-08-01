package com.hpsk.zhizuobiao.Bigdata.mapreduce.uv;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UvMrMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	private Text outputKey = new Text();
	private IntWritable outputValue = new IntWritable();
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString();
		String[] items = line.split("\t");
		String guid = items[5];
		String url = items[1];
		if(items.length>=36) {
			if(StringUtils.isNotBlank(guid) && StringUtils.isNotBlank(url)) {
				this.outputKey.set(guid);
				this.outputValue.set(1);
				//将每个用户ID输出
				context.write(outputKey, outputValue);
			}
		}else {
			//该数据直接丢弃，取下一条数据
			return;
		}
	}
	
}
