package com.hpsk.zhizuobiao.Bigdata.mapreduce.hv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class HouseWritable implements WritableComparable<HouseWritable> {
	private int num;
	private Double averagePrice;
	private Double maxPrice;
	private Double minPrice;
	
	public HouseWritable() {
		super();
		// TODO Auto-generated constructor stub
	}

	public HouseWritable(int num, Double averagePrice, Double maxPrice, Double minPrice) {
		super();
		this.num = num;
		this.averagePrice = averagePrice;
		this.maxPrice = maxPrice;
		this.minPrice = minPrice;
	}


	public int getNum() {
		return num;
	}

	public void setNum(int num) {
		this.num = num;
	}

	public Double getAveragePrice() {
		return averagePrice;
	}

	public void setAveragePrice(Double averagePrice) {
		this.averagePrice = averagePrice;
	}

	public Double getMaxPrice() {
		return maxPrice;
	}

	public void setMaxPrice(Double maxPrice) {
		this.maxPrice = maxPrice;
	}

	public Double getMinPrice() {
		return minPrice;
	}

	public void setMinPrice(Double minPrice) {
		this.minPrice = minPrice;
	}

	@Override
	public String toString() {
		return   num + "\t" + averagePrice + "\t"
				+ maxPrice + "\t" + minPrice;
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(this.num);
		out.writeDouble(this.averagePrice);
		out.writeDouble(this.maxPrice);
		out.writeDouble(this.minPrice);
		
		
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.num=in.readInt();
		this.averagePrice=in.readDouble();
		this.maxPrice=in.readDouble();
		this.minPrice=in.readDouble();
		
	}

	public int compareTo(HouseWritable o) {
		int comp=Integer.valueOf(this.getNum()).compareTo(o.getNum());
		if (comp==0) {
			int comp1=this.getAveragePrice().compareTo(o.getAveragePrice());
			if(comp1==0) {
				int comp2= this.getMaxPrice().compareTo(o.getMaxPrice());
				if(comp2==0) {
					return this.getMinPrice().compareTo(o.getMinPrice());
				}else {
					return comp2;
				}
			}else {
				return comp1;
			}
		} else {
		return comp;
		}
	}
	
	
	
	
	
}
