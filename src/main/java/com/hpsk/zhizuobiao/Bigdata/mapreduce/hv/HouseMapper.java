package com.hpsk.zhizuobiao.Bigdata.mapreduce.hv;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HouseMapper extends Mapper<LongWritable, Text, Text, HouseWritable> {
	private Text outputKey = new Text();
	private HouseWritable outputValue = new HouseWritable();
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, HouseWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString();
		String[] items = line.split(",");
		String locate = items[3];
		String price = items[7];
		if(items.length>=9) {
			if(StringUtils.isNotBlank(locate)&&StringUtils.isNotBlank(price)) {
				outputKey.set(locate);
				outputValue.setNum(1);
				outputValue.setAveragePrice(Double.valueOf(price));
				outputValue.setMaxPrice(Double.valueOf(price));
				outputValue.setMinPrice(Double.valueOf(price));
				context.write(outputKey, outputValue);
			}
		}
		
	}
	
}
