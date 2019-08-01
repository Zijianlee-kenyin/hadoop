package com.zhizuobiao.bigdata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;

/**
 * HDFSde JAVA api
 * @author admin
 *
 */
public class HDFSAPI {
	
	private static FileSystem fs;
	
	static {
		//所有的Hadoop程序都需要一个configuration对象，来管理当前程序的所有配置
		Configuration conf = new Configuration();
		//conf.set("");
		try {
			fs = FileSystem.get(new URI("hdfs://192.168.213.134:8020"), conf, "hadoop");
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
	
	/**
	 * 打印集群中datanode的信息
	 * @throws IOException
	 */
	private static void testClusterInfo() throws IOException {
		//将普通的文件系统转换hdfs的文件系统
		DistributedFileSystem hdfs=(DistributedFileSystem)fs;
		DatanodeInfo[] dns=hdfs.getDataNodeStats();
		for(DatanodeInfo dn:dns) {
			System.out.println("datanode:\t"+dn.getHostName()+"\t"+dn.getDatanodeUuid());
		}
	}
	
	/**
	 * 列举目录
	 * @throws FileNotFoundException
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	private static void testList() throws FileNotFoundException, IllegalArgumentException, IOException {
		String name = null;
		FileStatus[] status=fs.listStatus(new Path("/"));
		for(FileStatus sta:status) {
			name=sta.getPath().getName();
			if(sta.isDirectory()) {
				System.out.println(name+"\t"+"d");
			}else {
				System.out.println(name+"\t"+"f");
			}
		}
	}
	
	private static void readFile(String remoteFilePath)  {
		FSDataInputStream in = null;
		try {
			in = fs.open(new Path(remoteFilePath));
			IOUtils.copyBytes(in, System.out, 4096, false);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			 if(in != null){
	                IOUtils.closeStream(in);
	            }
		}
	}
	
	private static void loadFile(String localPath, String hdfsPath) {
		FSDataOutputStream outputStream = null;
        FileInputStream fileInputStream = null;
        Path path = new Path(hdfsPath);
        try {
			outputStream = fs.create(path);
			fileInputStream = new FileInputStream(new File(localPath));
			IOUtils.copyBytes(fileInputStream, outputStream, 4096, false);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(outputStream!=null) {
				IOUtils.closeStream(outputStream);
			}
			if(fileInputStream!=null) {
				IOUtils.closeStream(fileInputStream);
			}
		}
        
	}
	
	private static void deleteFile(String hdfsPath) {
		try {
			fs.delete(new Path(hdfsPath), false);
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
//		testClusterInfo();
//		testList();
//		readFile("/data/input/hello.txt");
//		loadFile("C:\\Users\\87645\\Desktop\\hello2.txt", "/data/input/hello2.txt");
		deleteFile("/data/input/hello");

	}

}
