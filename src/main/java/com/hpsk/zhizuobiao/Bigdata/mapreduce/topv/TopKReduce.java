package com.hpsk.zhizuobiao.Bigdata.mapreduce.topv;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class TopKReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	private final int k =10;
	private TreeMap<MyInt,String> tm = new TreeMap<MyInt,String>(new Comparator<MyInt>() {

		public int compare(MyInt o1, MyInt o2) {
			// TODO Auto-generated method stub
			return o2.compareTo(o1);
		}
		
		
	});
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int sum = 0;
		for(IntWritable value:values) {
			
			sum +=value.get();
			
		}
		tm.put(new MyInt(sum), key.toString());
		if(tm.size()>k) {
			tm.remove(tm.lastKey());
		}
	}
	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String ouk="";
		IntWritable ouv= new IntWritable();
		for(Map.Entry<MyInt,String> entry:tm.entrySet()) {
			ouk=(String)entry.getValue();
			int i=entry.getKey().getValue();
			ouv.set(i);
			context.write(new Text(ouk), ouv);
		}
	}
	
	
	
	
}
