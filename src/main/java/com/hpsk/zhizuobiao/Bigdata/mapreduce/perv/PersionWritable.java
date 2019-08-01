package com.hpsk.zhizuobiao.Bigdata.mapreduce.perv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;



public class PersionWritable implements  WritableComparable<PersionWritable> {
	private String name;
	private int num;
	
	
	

	public PersionWritable(String name, int num) {
		super();
		this.name = name;
		this.num = num;
	}

	public PersionWritable() {
		super();
		// TODO Auto-generated constructor stub
	}
	
	

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getNum() {
		return num;
	}

	public void setNum(int num) {
		this.num = num;
	}
	
	

	@Override
	public String toString() {
		return  name + "\t" + num ;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.name);
		out.writeInt(this.num);

	}

	public void readFields(DataInput in) throws IOException {
		this.name=in.readUTF();
		this.num=in.readInt();

	}

	public int compareTo(PersionWritable p) {
		int comp=this.getName().compareTo(p.getName());
		if(comp==0) {
			return Integer.valueOf(this.getNum()).compareTo(p.getNum());
		}
		return comp;
	}

}
