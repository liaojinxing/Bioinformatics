package utils;

public class TripleTool {
	public static Triple parseLineToTriple(String line){
		String[] resources = line.split("\\s+");
		if(resources.length == 3){
			String subject = resources[0];
			String predicate = resources[1];
			String object = resources[2];
			Triple triple = new Triple(subject,predicate,object, false);
			return triple;
		}
		return null;
	}
}
