package com.hpsk.zhizuobiao.Bigdata.mapreduce.perv;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PersionReduce extends Reducer<Text, PersionWritable, Text, PersionWritable> {
	private PersionWritable outputValue = new PersionWritable();
	@Override
	protected void reduce(Text key, Iterable<PersionWritable> values,
			Reducer<Text, PersionWritable, Text, PersionWritable>.Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for(PersionWritable val:values) {
			sum += val.getNum();
			outputValue.setName(val.getName());
		}
		outputValue.setNum(sum);
		context.write(key, outputValue);
	}

}
