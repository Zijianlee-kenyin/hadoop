package com.hpsk.zhizuobiao.Bigdata.cstv;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopnMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text outputKey = new Text();
	private IntWritable outputValue = new IntWritable();
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] items = line.split("\t");
		String keyword = items[2];
		if(items.length>=5) {
			outputKey.set(keyword);
			outputValue.set(1);
			context.write(outputKey, outputValue);
		}else {
			return;
		}
	}
	
}
