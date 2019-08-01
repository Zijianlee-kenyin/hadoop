package com.hpsk.zhizuobiao.Bigdata.mapreduce.perv;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PersionMapper extends Mapper<LongWritable, Text, Text, PersionWritable> {
	private Text outputKey = new Text();
	private PersionWritable outputValue = new PersionWritable();
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, PersionWritable>.Context context)
			throws IOException, InterruptedException {
		String line = new String(value.getBytes(),"GBK");
		String[] items = line.split("\t");
		String sno = items[0];
		String name = items[1];
		if(items.length>=4) {
			if("10001".equals(sno) && StringUtils.isNotBlank(name)) {
				outputKey.set(sno);
				outputValue.setName(name);
				outputValue.setNum(1);
				context.write(outputKey, outputValue);
			}
		}
	}
	
}
