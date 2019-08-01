package bigdatademo.hadoop;

public class test {

	public static void main(String[] args) {
		String line = "00:00:00	2982199073774412	[360安全卫士]	8 3	download.it.com.cn/softweb/software/firewall/antivirus/20067/17938.html";
		String[] items = line.split("\t");
		for(String val:items) {
			System.out.println(val);
		}

	}

}
