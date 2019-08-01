package com.hpsk.zhizuobiao.Bigdata.mapreduce.topv;

public class MyInt implements Comparable<MyInt> {
	private Integer value;
	
	

	public MyInt(Integer value) {
		this.value = value;
	}

    

	public Integer getValue() {
		return value;
	}



	public void setValue(Integer value) {
		this.value = value;
	}



	public int compareTo(MyInt arg0) {
		// TODO Auto-generated method stub
		return value.compareTo(arg0.getValue());
	}

}
