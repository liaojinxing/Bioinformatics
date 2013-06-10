package preprocess;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class virtuosotest {
	private static final String OUTPUTFILE = "/home/ljx/thesis/data/TCMGeneDIT/TCMGeneDIT_triple";
	
	public static void WriteTripleToFile(ResultSet rs, String outputFileName)
	 {
	   try {
		 //File file = new File(OUTPUTFILE);
		 File file = new File(outputFileName);
		 BufferedWriter bw = new BufferedWriter(new FileWriter(file, true));
	     ResultSetMetaData rsmd;

	     System.out.println(">>>>>>>>");
	     rsmd = rs.getMetaData();
	     int cnt = rsmd.getColumnCount();

	       while(rs.next()) {
	         Object o;

	         for (int i = 1; i <= cnt; i++) {
	           o = rs.getObject(i);
	           if (rs.wasNull())
	             System.out.print("<NULL> ");
	           else{
	             //System.out.print(o + "\t");
	             bw.write(o+"\t");
	           }
	         }
	         bw.newLine();
	         //System.out.println();
	       }
	       bw.close();
	   } catch (Exception e) {
	     System.out.println(e);
	     e.printStackTrace();
	   }
	   System.out.println(">>>>>>>>");   
	 }
	
	public static void ConvertRDFToTriple(String graphPrefix, List<String> graphNameList){
		try{
			String url = "jdbc:virtuoso://10.214.0.144:1111/UID=dba/PWD=dba";
			Class.forName("virtuoso.jdbc4.Driver");
			Connection conn = DriverManager.getConnection(url);
			Statement stmt = conn.createStatement();
			
	//		for(String graphName:graphNameList){
				String sql = "SPARQL SELECT distinct ?h WHERE {<http://purl.org/net/tcm/tcm.lifescience.ntu.edu.tw/id/medicine/Ganoderma_lucidum> <http://purl.org/net/tcm/tcm.lifescience.ntu.edu.tw/treatment> ?b . ?b <http://www.w3.org/2002/07/owl#sameAs> ?c .?c <http://www4.wiwiss.fu-berlin.de/diseasome/resource/diseasome/possibleDrug> ?d .?d <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/target> ?e .?e <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/swissprotId> ?f .?f <http://purl.uniprot.org/core/classifiedWith> ?g .?g <http://www.ccnt.org/symbol> ?h.};";
				ResultSet rs = stmt.executeQuery(sql);
				//ResultSet rs = stmt.executeQuery("SPARQL select * from <"+graphPrefix+graphName+"> where {?s ?p ?o}");
				//String filePrefix = "/home/ljx/thesis/reason_data/";
				WriteTripleToFile(rs, "/home/ljx/thesis/reason_data/lingzhi");
			//	prnRs(rs, filePrefix + graphName);
		//	}
			
		}catch (Exception e) {
			System.out.println(e.toString());
		}
	}
	
	public static void AppendSimple() throws IOException{
		
		String inputPrefix = "/home/ljx/thesis/data/TCMGeneDIT/simple/";
		ArrayList<String> fileNameList = new ArrayList<String>();
		fileNameList.add("diseases_tcm_dbpedia_simple.owl");
		
		fileNameList.add("diseases_tcm_diseasesome_simple.owl");
		fileNameList.add("diseases_tcm_sideeffect_simple.owl");
		fileNameList.add("drugs_tcm_drugbank_simple.owl");
		fileNameList.add("genes_tcm_dbpedia_simple.owl");
		fileNameList.add("genes_tcm_diseasesome_simple.owl");
		fileNameList.add("genes_tcm_drugbank_simple.owl");
		fileNameList.add("herbspecies_tcm_dbpedia_simple.owl");
		fileNameList.add("ingredients_tcm_dailymed_simple.owl");

		File outputFile = new File(OUTPUTFILE);
		BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile, true));
		
		for(String fileName:fileNameList){
			File file = new File(inputPrefix + fileName);
			BufferedReader reader = new BufferedReader(new FileReader(file));
			
			String line = "";
			while((line=reader.readLine())!=null){
				String[] elements = line.split("\\s+");
				bw.write(elements[0].substring(1, elements[0].length()-1)+"\t"
						+ elements[1].substring(1, elements[1].length()-1)+"\t"
						+ elements[2].substring(1, elements[2].length()-1));
				bw.newLine();
			}
			reader.close();
		}
		bw.flush();
		bw.close();
	}
	
	
	public static void main(String[] args) throws IOException{
		ArrayList<String> graphList = new ArrayList<String>();
		graphList.add("GeneOntology");
		
		ConvertRDFToTriple("http://localhost:8890/", graphList);
		//graphList.add("TCM_disease_associations_statistics");
		//graphList.add("TCM_ingredient_associations_statistics");
		//graphList.add("TCM_gene_associations_statistics");
		//graphList.add("TCM_gene_disease_associations_statistics");
		//ConvertRDFToTriple("http://localhost:8890/", graphList);
		//AppendSimple();
		
	}
}
