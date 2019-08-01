package com.hpsk.zhizuobiao.Bigdata.mapreduce.topv;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopnReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	private HashMap<Text,Integer> putmap = new HashMap<Text,Integer>();
	private ValueComparator bvc = new ValueComparator();
	private TreeMap<Text,Integer> outputmap = null;
	private TreeMap<Integer,Text> temmap = new TreeMap<Integer, Text>(new Comparator<Integer>() {

		public int compare(Integer o1, Integer o2) {
			// TODO Auto-generated method stub
			return o2-o1;
		}
		
	});
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Integer sum = 0;
		for(IntWritable value:values) {
			sum +=value.get();
		}
		temmap.put(sum, key);
		
		
	}
	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		for(Map.Entry<Integer,Text> val:temmap.entrySet()) {
			context.write(val.getValue(),new IntWritable(val.getKey()));
		}
		
	}
	
	class ValueComparator implements Comparator<Text>{
		Map<Text,Integer> base;
		

		public Map<Text, Integer> getBase() {
			return base;
		}

		public void setBase(Map<Text, Integer> base) {
			this.base = base;
		}
		
		

		public ValueComparator() {
			super();
			// TODO Auto-generated constructor stub
		}

		public ValueComparator(Map<Text, Integer> base) {
			super();
			this.base = base;
		}

		public int compare(Text a, Text b) {
			// TODO Auto-generated method stub
			if(base.get(a) >= base.get(b)) {
				return -1;
			}else {
				return 1;
			}
		}
		
	}
	
	
	
	
	
}
