package preprocess;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class TrigProducer {
	
	public static void UniProtTrigProducer(String graphName, String fileName) throws IOException{
		File file = new File(fileName);
		BufferedReader reader = new BufferedReader(new FileReader(file));

		File outputfile = new File(fileName+".trig");
		BufferedWriter bw = new BufferedWriter(new FileWriter(outputfile));
		
		//bw.write("<http://uniprot.org/protein_gene_mapping> {");
		bw.write("<"+graphName+"> {");
		bw.newLine();
		
		String line = "";
		while((line=reader.readLine())!=null){
			String[] elements = line.split("\\s+");
			if(elements.length!=3)
				continue;
			/*
			elements[0] = elements[0].replaceAll("http://purl.obolibrary.org/obo/", "http://purl.org/obo/owl/GO#");
			
			if(elements[2].length()>=4 && elements[2].substring(0, 4).equals("http")){
				bw.write("\t"+"<"+elements[0]+"> <"+elements[1]+"> <"+elements[2]+"> .");
			}else{
				String temp = elements[2].replaceAll("\"", "");
				temp = temp.replaceAll("\\\\", "");
				bw.write("\t"+"<"+elements[0]+"> <"+elements[1]+"> \""+temp+"\" .");
			}
			*/		
			
			elements[0]="<"+elements[0]+">";
			elements[1]="<"+elements[1]+">";
			elements[2]="<"+elements[2]+">";
			if(elements[2].contains("\\")){
				System.out.println(elements[2]);
				continue;
			}
			bw.write("\t"+elements[0]+" "+elements[1]+" "+elements[2]+" .");
			bw.newLine();
		}
		
		bw.write("}");
		bw.newLine();
		
		bw.flush();
		bw.close();	
		reader.close();
	}

	public static void main(String[] args) throws IOException{
		for(int i = 2;i<8;i++){
			UniProtTrigProducer("http://localhost:8890/TCMGeneDIT","/home/ljx/thesis/reason_data/TCMGeneDIT_triple");
			System.out.println(i);
		}
	}
	
	
}
