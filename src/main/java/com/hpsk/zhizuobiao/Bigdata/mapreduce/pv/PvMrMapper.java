package com.hpsk.zhizuobiao.Bigdata.mapreduce.pv;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PvMrMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text outputKey = new Text();
	private IntWritable outputValue = new IntWritable();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		/**
		 * 将数据进行非法性判断
		 * 1-长度小于36个字符
		 * 2-省份ID为空
		 * 3-url为空
		 */
		String line = value.toString();
		String[] items = line.split("\t");
		String provinceId = items[23];
		String url = items[1];
		if(items.length>=36) {
			//判断该数据的省份id和url是否为空
			if(StringUtils.isNotBlank(provinceId) && StringUtils.isNotBlank(url)) {
				this.outputKey.set(provinceId);
				//将每个省份ID输出
				context.write(outputKey, outputValue);
			}
		}else {
			//该数据直接丢弃，取下一条数据
			return;
		}
	}
	
}
