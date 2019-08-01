package com.hpsk.zhizuobiao.Bigdata.mapreduce.iv;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IvMrMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text outputKey = new Text();
	private IntWritable outputValue = new IntWritable();
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString();
		String[] items = line.split("\t");
		String url = items[1];
		String ip = items[12];
		String time = items[17];
		if(items.length>=36) {
			if(StringUtils.isNotBlank(ip) && StringUtils.isNotBlank(url) && StringUtils.isNotBlank(time)) {
				this.outputKey.set(time);
				this.outputValue.set(1);
				//将每个用户ID输出
				context.write(outputKey, outputValue);
			}
		}
	}
	
}
