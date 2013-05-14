package utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class OWLRuleChainUtil {
	private static String LOCALFILE="/user/ljx/rulechain";
	private static String LOCALHDFS="hdfs://localhost:9000";
	
	private static String CLUSTERFILE="/user/root/rulechain";
	private static String CLUSTERHDFS="hdfs://192.168.0.236:9000";
	
	private static String hdfsAddr=CLUSTERHDFS;
	private static String rulechainFileAddr=CLUSTERFILE;
	
	// 存储rulechain所涉及到的全部predicate
	private static LinkedList<String> ruleChainLst = new LinkedList<String>();
	
	// 从HDFS读取和写入数据的类
	private static OWLHDFSUtil owlFSTool = new OWLHDFSUtil(hdfsAddr);
	// 临时用于去除重复数据，必须用一个全局性的变量，写到文件里
	// 记录输出的结果，用于剔出重复数据，仅仅是在一个reducer中做到去重
	public static List<String> proLst = new ArrayList<String>();
	
	
	// 将rulechanin的所有内容载入内存，只需要读取初始的一次就可以了，之后所有的都已经载入到内存中了
	static{	
		/*
		ruleChainLst.add("http://purl.org/net/tcm/tcm.lifescience.ntu.edu.tw/treatment");
		ruleChainLst.add("http://www.w3.org/2002/07/owl#sameAs");
		ruleChainLst.add("http://www4.wiwiss.fu-berlin.de/diseasome/resource/diseasome/possibleDrug");
		ruleChainLst.add("http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/target");
		ruleChainLst.add("http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/swissprotId");
		ruleChainLst.add("http://purl.uniprot.org/core/classifiedWith");
		//ruleChainLst.add("http://www.w3.org/2000/01/rdf-schema#label");
		ruleChainLst.add("http://www.ccnt.org/symbol");
		*/
		ruleChainLst.clear();
		BufferedReader br=owlFSTool.readFile(rulechainFileAddr);
		String tmp="";
		try {
			while((tmp=br.readLine())!=null)
				ruleChainLst.add(tmp);
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		tmp=null;
		br=null;
		
	}
	
	//根据predicate的值获取它的index
	public static int getIndexByPredicate(String pre){
//		System.out.println("key:"+pre);
		return ruleChainLst.indexOf(pre);
	}

	// 获取现存predicate的数目
	public static int getPredicateNum() {
		return ruleChainLst.size();
	}

	// 更新predicate的数组，操作是将临近的两个单位合并为一个
	public static int updatePredicateArr() {
		LinkedList<String> tmpLst=new LinkedList<String>();
		int size=ruleChainLst.size();
		if(size==1)
			return 1;
		for(int i=0;i<size;i=i+2){
			// 得到第一个predicate的值
			String p1=ruleChainLst.get(i);
			// 当size为奇数时，直接加入
			if(i+1>=size){
				tmpLst.add(p1);
				break;
			}
			String p2=ruleChainLst.get(i+1);
			tmpLst.add(p1+"$$"+p2);
		}
		ruleChainLst.removeAll(ruleChainLst);
		ruleChainLst=tmpLst;
//		System.out.println(ruleChainLst.size());
		return ruleChainLst.size();
		
	}
	
	// 将变更写入文件，为下次job作准备，写脚本时使用
	
	public static void RefreshRuleList(){
		ruleChainLst.clear();
		BufferedReader br=owlFSTool.readFile(rulechainFileAddr);
		String tmp="";
		try {
			while((tmp=br.readLine())!=null)
				ruleChainLst.add(tmp);
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		tmp=null;
		br=null;
	}
	
	
	public static void updatePredicateArrIntoFile() {
		int size=ruleChainLst.size();
		String writeCon="";
		for(int i=0;i<size;i=i+2){
			// 得到第一个predicate的值
			String p1=ruleChainLst.get(i);
			// 当size为奇数时，直接加入
			if(i+1>=size){
				writeCon+=p1+"\n";
				break;
			}
			String p2=ruleChainLst.get(i+1);
			writeCon+=p1+"$$"+p2+"\n";
		}
		//更新完后，写回文件，为下一次job做准备
		owlFSTool.writeFile(rulechainFileAddr, writeCon);
		//return ruleChainLst.size();
	}
	
	//由index得到相应的predicate
	public static String getPredicateByIndex(int i) {
		return ruleChainLst.get(i);
	}
	


}
