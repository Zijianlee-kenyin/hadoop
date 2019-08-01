package com.hpsk.zhizuobiao.Bigdata.mapreduce.hv;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HouseReduce extends Reducer<Text, HouseWritable, Text, HouseWritable> {
	private HouseWritable outputValue = new HouseWritable();
	
	
	@Override
	protected void reduce(Text key, Iterable<HouseWritable> values,
			Reducer<Text, HouseWritable, Text, HouseWritable>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		outputValue.setNum(0);
		outputValue.setAveragePrice(0.00);
		outputValue.setMaxPrice(0.00);
		outputValue.setMinPrice(99999999999999999.00);
		int numNew=0;
		Double averagePrice=0.00;
		Double sumNew=0.00;
		for(HouseWritable val:values) {
			System.out.println(val.toString());
			if(outputValue.getMaxPrice()<val.getMaxPrice()) {
				outputValue.setMaxPrice(val.getMaxPrice());
			}
			
			if(outputValue.getMinPrice()>val.getMinPrice()) {
				outputValue.setMinPrice(val.getMinPrice());
			}
			sumNew +=val.getAveragePrice()*val.getNum();
			numNew +=val.getNum();
		}
		outputValue.setNum(numNew);
		averagePrice=Double.valueOf(Math.round(sumNew/numNew));
		outputValue.setAveragePrice(averagePrice);
		context.write(key, outputValue);
	}
	
}
