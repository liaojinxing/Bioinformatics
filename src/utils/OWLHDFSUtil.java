package utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public class OWLHDFSUtil {

	private FileSystem fs=null;
	
	public OWLHDFSUtil(String hdfsSrv){
		try {
			this.fs=DistributedFileSystem.get(URI.create(hdfsSrv), new Configuration());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/*
	 * ���ļ�������һ��BufferedReader��ʵ����Ե������readline����ȡÿ�е����
	 */
	public BufferedReader readFile(String filePath){
		FSDataInputStream in=null;
		try {
			in=this.fs.open(new Path(filePath));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new BufferedReader(new InputStreamReader(in));
	}
	
	
	/*
	 * д�ļ�
	 */
	public void writeFile(String filePath,String content){
		FSDataOutputStream out=null;
		try {
			out=this.fs.create(new Path(filePath));
			out.write(content.getBytes());
			out.flush();
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
}
