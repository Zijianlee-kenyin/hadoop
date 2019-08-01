package com.hpsk.zhizuobiao.Bigdata.mapreduce.mysql;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

public class TaobaoWritable implements Writable, DBWritable {
	
	private String province;
	private int userNum;
	private int maleNum;
	private int femaleNum;
	private int orderNum;
	
	public TaobaoWritable(){
		super();
	}
	


	public String getProvince() {
		return province;
	}



	public void setProvince(String province) {
		this.province = province;
	}



	public int getUserNum() {
		return userNum;
	}



	public void setUserNum(int userNum) {
		this.userNum = userNum;
	}



	public int getMaleNum() {
		return maleNum;
	}



	public void setMaleNum(int maleNum) {
		this.maleNum = maleNum;
	}



	public int getFemaleNum() {
		return femaleNum;
	}



	public void setFemaleNum(int femaleNum) {
		this.femaleNum = femaleNum;
	}



	public int getOrderNum() {
		return orderNum;
	}



	public void setOrderNum(int orderNum) {
		this.orderNum = orderNum;
	}



	public void write(PreparedStatement statement) throws SQLException {
		// TODO Auto-generated method stub
		statement.setString(1, this.province);
		statement.setInt(2, this.userNum);
		statement.setInt(3, this.maleNum);
		statement.setInt(4, this.femaleNum);
		statement.setInt(5, this.orderNum);
	}

	public void readFields(ResultSet resultSet) throws SQLException {
		// TODO Auto-generated method stub
		this.province = resultSet.getString(1);
		this.userNum = resultSet.getInt(2);
		this.maleNum = resultSet.getInt(3);
		this.femaleNum = resultSet.getInt(4);
		this.orderNum = resultSet.getInt(5);
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(this.province);
		out.writeInt(this.userNum);
		out.writeInt(this.maleNum);
		out.writeInt(this.femaleNum);
		out.writeInt(this.orderNum);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.province = in.readUTF();
		this.userNum = in.readInt();
		this.maleNum  = in.readInt();
		this.femaleNum = in.readInt();
		this.orderNum = in.readInt();
	}

}
