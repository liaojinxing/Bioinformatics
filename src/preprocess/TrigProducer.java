package preprocess;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class TrigProducer {
	/***
	 * Produce trig file for uploading
	 * @param graphName
	 * @param fileName
	 * @throws IOException
	 */
	public static void TrigGraphProducer(String graphName, String fileName) throws IOException{
		File file = new File(fileName);
		BufferedReader reader = new BufferedReader(new FileReader(file));

		File outputfile = new File(fileName+".trig");
		BufferedWriter bw = new BufferedWriter(new FileWriter(outputfile));

		bw.write("<"+graphName+"> {");
		bw.newLine();
		
		String line = "";
		while((line=reader.readLine())!=null){
			String[] elements = line.split("\\s+");
			if(elements.length!=3)
				continue;
			
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
		TrigGraphProducer("http://localhost:8890/ReasonResult","/home/ljx/thesis/reason_data/result");
	}
	
	
}
