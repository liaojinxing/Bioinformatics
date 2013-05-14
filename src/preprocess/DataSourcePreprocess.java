package preprocess;

import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.util.FileManager;
import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataSourcePreprocess {
	
	static HashMap<String, Long> diseaseSet = new HashMap<String, Long>();
	static HashMap<String, Long> medicineSet = new HashMap<String, Long>();
	
	/**
	 * convert rdf file to triple
	 * @param filePath
	 * @param fileName
	 * @param propFilter
	 * @param reverseSubObj
	 * @throws IOException
	 */
	public static void ConvertFileToTriple(String filePath, String fileName, Set<String> propFilters, boolean reverseSubObj) throws IOException{
		String inputFile = filePath + fileName;
		Model model = ModelFactory.createDefaultModel();
		
		InputStream in = FileManager.get().open(inputFile);
		if (in == null) {
		    throw new IllegalArgumentException("File not found");
		}
		
		String outputFileName = filePath + "/" + fileName.substring(0,fileName.indexOf('.')) + "_triple"; 
		File file = new File(outputFileName);
		BufferedWriter bw = new BufferedWriter(new FileWriter(file));
		
		model.read(in,null);
		Property predicate = null;
		Resource subject = null;
		RDFNode object = null;
		StmtIterator iterator = model.listStatements();
		Statement stmt = null;
		while(iterator.hasNext()){
			stmt = iterator.next();
			subject = stmt.getSubject();
			predicate = stmt.getPredicate();
			object = stmt.getObject();
			if(propFilters.size()==0 || propFilters.contains(predicate.getURI().trim())){
			//if (propFilter.equals("") || predicate.getURI().trim().equals(propFilter)){
				if (reverseSubObj)
					bw.write(object.toString()+"\t"+predicate.getURI()+"\t"+subject.getURI());
				else
					bw.write(subject.getURI()+"\t"+predicate.getURI()+"\t"+object.toString());
				bw.newLine();
			}
		}
		bw.flush();
		bw.close();
	}
		
	public static void SimplizeTriple(String fileName) throws IOException{
		File file = new File(fileName);
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String tvalue = "";
		String disease = "";
		String medicine = "";
		
		File outputfile = new File(fileName+"_simple");
		BufferedWriter bw = new BufferedWriter(new FileWriter(outputfile));
				
		Long i = new Long(1);
		Long j = new Long(1);
		
		while(true){
			tvalue = reader.readLine();
			if(tvalue == null)
				break;
			tvalue = tvalue.split("\t")[2];
			disease = reader.readLine().split("\t")[2];
			medicine = reader.readLine().split("\t")[2];
			
			//1.414^^http://www.w3.org/2001/XMLSchema#float
			tvalue = tvalue.substring(0,tvalue.indexOf('^'));
			//http://purl.org/net/tcm/tcm.lifescience.ntu.edu.tw/id/disease/Enteritis
			disease = disease.substring(disease.indexOf("disease")+8);
			//http://purl.org/net/tcm/tcm.lifescience.ntu.edu.tw/id/medicine/Rangifer_tarandus
			medicine = medicine.substring(medicine.indexOf("medicine")+9);
			
			if(!medicineSet.containsKey(medicine)){
				medicineSet.put(medicine, i++);
			}
			if(!diseaseSet.containsKey(disease)){
				diseaseSet.put(disease, j++);
			}
			
			bw.write(medicineSet.get(medicine)+","+diseaseSet.get(disease)+","+tvalue);
			bw.newLine();
		}
		bw.flush();
		bw.close();
				
	}
	
	public static void writeMapping(String fileName) throws IOException{
		File mappingFile = new File(fileName+"_mapping");
		BufferedWriter mappingWriter = new BufferedWriter(new FileWriter(mappingFile));
		Iterator<Map.Entry<String, Long>> iter = medicineSet.entrySet().iterator();
		while (iter.hasNext()) {
		    Map.Entry entry = (Map.Entry) iter.next();
		    mappingWriter.write("med:"+entry.getKey()+":"+entry.getValue());
		    mappingWriter.newLine();
		} 
		Iterator<Map.Entry<String, Long>> disIter = diseaseSet.entrySet().iterator();
		while (disIter.hasNext()) {
		    Map.Entry entry = (Map.Entry) disIter.next();
		    mappingWriter.write("dis:"+entry.getKey()+":"+entry.getValue());
		    mappingWriter.newLine();
		}
		
		mappingWriter.flush();
		mappingWriter.close();	
	}	
	
	public static void TranslateFile(String fileName) throws IOException {
		File file = new File(fileName);
		BufferedReader reader = new BufferedReader(new FileReader(file));

		File outputfile = new File("/home/ljx/thesis/data/symbol_geneid_mapping_triple");
		BufferedWriter bw = new BufferedWriter(new FileWriter(outputfile, true));
		
		String line = "";
		
	/*	
		<rdf:Description rdf:about="http://biotcm_cloud/gene/8397766">
	    <owl:sameAs>http://purl.org/net/tcm/tcm.lifescience.ntu.edu.tw/id/gene/Apre_0980</owl:sameAs>
	  </rdf:Description>
	  */  
		while((line=reader.readLine())!=null){
			Pattern pattern = Pattern.compile("<rdf:Description rdf:about=.*>");
			Matcher matcher = pattern.matcher(line);
			if(matcher.find()){
				String matchString = matcher.group(0);
				String secondLine = reader.readLine();
				String subject = matchString.substring(matchString.indexOf("\"")+1, matchString.length()-2);
				subject = subject.replaceAll("http://biotcm_cloud/gene/", "http://purl.org/commons/record/ncbi_gene/");
				String predicate = "http://www.ccnt.org/symbol";

				String object = secondLine.substring(secondLine.indexOf("<owl:sameAs>")+12,secondLine.indexOf("</owl:sameAs>"));
				bw.write(subject + "\t" + predicate +"\t"+object);
				bw.newLine();
			}
		}
		
		bw.flush();
		bw.close();	
		reader.close();
	}
	
	public static void SplitFile(String srcFile, long lineCount) throws IOException{
		File file = new File(srcFile);
		BufferedReader reader = new BufferedReader(new FileReader(file));
	
		String line = null;
		long count = 0;
		int iterNum = 0;
		
		File outputfile = new File("/home/ljx/thesis/data/symbol_geneid_mapping_triple0");
		BufferedWriter bw = new BufferedWriter(new FileWriter(outputfile, true));
//		System.out.println(reader.readLine());
		do{
			line=reader.readLine();
		}while(line!=null&&line.length()>0);
//		while((line=reader.readLine())!=null){			
//			bw.write(line);
//			bw.newLine();
//			count++;
//			if(count>lineCount){
//				bw.flush();
//				bw.close();
//				iterNum++;
//				count = 0;
//				outputfile = new File("/home/ljx/thesis/data/symbol_geneid_mapping_triple"+iterNum);
//				bw = new BufferedWriter(new FileWriter(outputfile, true));
//			}
			
//		}
		
		bw.flush();
		bw.close();	
		reader.close();
	}
	
	
	
	public static void main(String[] args) throws IOException{
		/*String path = "/home/ljx/thesis/data/TCMGeneDIT/";
		String fileName = "TCM_disease_associations_statistics.rdf";
		String tvalue = "http://purl.org/net/tcm/tcm.lifescience.ntu.edu.tw/medicine_disease_tvalue";
		String source = "http://purl.org/net/tcm/tcm.lifescience.ntu.edu.tw/source";
		Set<String> filterSet = new HashSet<String>();
		filterSet.add(tvalue);
		filterSet.add(source);
		ConvertFileToTriple(path, fileName, filterSet, false);*/
		//SimplizeTriple("/home/ljx/thesis/data/TCMGeneDIT/TCM_disease_associations_statistics_triple");
		//writeMapping("/home/ljx/thesis/data/TCMGeneDIT/TCM_disease_associations_statistics_triple");
		/*	test5.rdf  test690440.rdf  test3430700.rdf  test5730901.rdf   test8397700.rdf  test8537301.rdf */
		//TranslateFile("/home/ljx/thesis/data/gene_symbol-id-mapping/test8537301.rdf");
		SplitFile("/media/文档/uniprot/uniprot_protein_GO_mapping_triple",9000000);
	}
}
