package reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utils.OWLRuleChainUtil;
import utils.Triple;
import utils.TripleTool;

public class GeneDiseaseReducer extends Reducer<Text, Text, NullWritable, Text> {

	protected void reduce(Text key, Iterable<Text> values,
			GeneDiseaseReducer.Context context) throws IOException,
			InterruptedException {
	
		Set<String> subjectURIs = new HashSet<String>();
		Set<String> objectURIs = new HashSet<String>();
		Triple outputTriple = new Triple();
		int index = 0;
		int current = 0;
	
		for(Text value : values) {
			Triple triple = TripleTool.parseLineToTriple(value.toString());
			index = OWLRuleChainUtil.getIndexByPredicate(triple.getPredicate());
			if(index==-1)
				return;
			current = index % 2;

			if((index+1)==OWLRuleChainUtil.getPredicateNum()&&current==0){
				context.write(NullWritable.get(), value);
				return;
			}
			
			//if the predicate's index is even, add the subject to subjectset
			// if it is odd, add the object to objectset 
			if (current==0) {
				subjectURIs.add(triple.getSubject());
			} else {
				objectURIs.add(triple.getObject());
			}
		}
				
		//join the two adjacent predicate to construct the produced predicate
		String outputPredicate = (current == 0) ? OWLRuleChainUtil.getPredicateByIndex(index)+"$$"+OWLRuleChainUtil
				.getPredicateByIndex(index + 1) : OWLRuleChainUtil
				.getPredicateByIndex(index - 1)+"$$"+OWLRuleChainUtil.getPredicateByIndex(index);
		outputTriple.setPredicate(outputPredicate);
		outputTriple.setObjectLiteral(false);
		
		Iterator<String> itr = objectURIs.iterator();
		while (itr.hasNext()) {
			outputTriple.setObject(itr.next());
			Iterator<String> itrSubclass = subjectURIs.iterator();
			while (itrSubclass.hasNext()) {
				outputTriple.setSubject(itrSubclass.next());
				context.write(NullWritable.get(), new Text(outputTriple.toString()));
			}
		}
		
	}
	
}
