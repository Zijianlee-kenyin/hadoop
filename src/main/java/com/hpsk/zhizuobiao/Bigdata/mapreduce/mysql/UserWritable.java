package com.hpsk.zhizuobiao.Bigdata.mapreduce.mysql;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class UserWritable implements WritableComparable<UserWritable> {

	private String uuid;
	private int sex;
	private int behavior;
	
	public UserWritable(){
		
	}
	
	public void setAll(String uuid,int sex,int behavior){
		this.setUuid(uuid);
		this.setSex(sex);
		this.setBehavior(behavior);
	}
	
	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public int getSex() {
		return sex;
	}

	public void setSex(int sex) {
		this.sex = sex;
	}

	public int getBehavior() {
		return behavior;
	}

	public void setBehavior(int behavior) {
		this.behavior = behavior;
	}

	@Override
	public String toString() {
		return uuid +"\t"+ sex+"\t"+behavior;
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(this.uuid);
		out.writeInt(this.sex);
		out.writeInt(behavior);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.uuid = in.readUTF();
		this.sex = in.readInt();
		this.behavior = in.readInt();
	}
	
	

	

	public int compareTo(UserWritable o) {
		// TODO Auto-generated method stub
		int comp = this.getUuid().compareTo(o.getUuid());
		if(comp == 0){
			int comp1 = Integer.valueOf(this.getSex()).compareTo(Integer.valueOf(o.getSex()));
			if(comp1 == 0){
				return Integer.valueOf(this.getBehavior()).compareTo(Integer.valueOf(o.getBehavior()));
			}else
				return comp1;
		}else
			return comp;
	}

}
