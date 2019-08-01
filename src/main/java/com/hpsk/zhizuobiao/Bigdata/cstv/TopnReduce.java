package com.hpsk.zhizuobiao.Bigdata.cstv;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.math3.util.MultidimensionalCounter.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopnReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable outputValue = new IntWritable();

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int sum = 0;
		for(IntWritable value:values) {
			sum +=value.get();
		}
		this.outputValue.set(sum);
		context.write(key, outputValue);
	}
	
	
	
}
