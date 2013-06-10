package utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class OWLRuleChainUtil {
	//private static String LOCALFILE="/user/ljx/rulechain";
	//private static String LOCALHDFS="hdfs://localhost:9000";
	
	private static String CLUSTERFILE="/user/root/rulechain";
	private static String CLUSTERHDFS="hdfs://192.168.1.236:9000";
	
	private static String hdfsAddr=CLUSTERHDFS;
	private static String rulechainFileAddr=CLUSTERFILE;
	
	// store the rule chain
	private static LinkedList<String> ruleChainLst = new LinkedList<String>();
	
	// tool class to read and write file in HDFS
	private static OWLHDFSUtil owlFSTool = new OWLHDFSUtil(hdfsAddr);

	// record the output result for deleting duplicates
	public static List<String> proLst = new ArrayList<String>();
	
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
			e.printStackTrace();
		}
		
		tmp=null;
		br=null;
		
	}
	
	/***
	 * get the predicate index
	 * @param pre
	 * @return index
	 */
	public static int getIndexByPredicate(String pre){
//		System.out.println("key:"+pre);
		return ruleChainLst.indexOf(pre);
	}

	/***
	 *  get the number of rules
	 * @return the rule chain length
	 */
	public static int getPredicateNum() {
		return ruleChainLst.size();
	}

	/***
	 *  update rule chain, merge the two ajacent rules
	 * @return the new chain length
	 */
	public static int updatePredicateArr() {
		LinkedList<String> tmpLst=new LinkedList<String>();
		int size=ruleChainLst.size();
		if(size==1)
			return 1;
		for(int i=0;i<size;i=i+2){
			// get the odd rule
			String p1=ruleChainLst.get(i);
			// add the last one if the rule size is odd
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

	/***
	 *  read rule chain file when beginning the next iteration
	 */
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
	
	/***
	 * update rule chain file 
	 */
	public static void updatePredicateArrIntoFile() {
		int size=ruleChainLst.size();
		String writeCon="";
		for(int i=0;i<size;i=i+2){
			String p1=ruleChainLst.get(i);

			if(i+1>=size){
				writeCon+=p1+"\n";
				break;
			}
			String p2=ruleChainLst.get(i+1);
			writeCon+=p1+"$$"+p2+"\n";
		}

		owlFSTool.writeFile(rulechainFileAddr, writeCon);
	}
	
	/***
	 * get the predicate according to the index
	 * @param i
	 * @return
	 */
	public static String getPredicateByIndex(int i) {
		return ruleChainLst.get(i);
	}
}
