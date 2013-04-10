package preprocess;

import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.util.FileManager;
import java.io.*;

public class DataSourcePreprocess {
	/**
	 * convert rdf file to triple
	 * @param filePath
	 * @param fileName
	 * @param propFilter
	 * @param reverseSubObj
	 * @throws IOException
	 */
	public static void ConvertFileToTriple(String filePath, String fileName, String propFilter, boolean reverseSubObj) throws IOException{
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
			if (propFilter.equals("") || predicate.getURI().trim().equals(propFilter)){
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
		
	public static void main(String[] args) throws IOException{
		String path = "/home/ljx/thesis/data/";
		String fileName = "rulechain";
		String treatment = "";
		ConvertFileToTriple(path, fileName, treatment, false);
	}
}
