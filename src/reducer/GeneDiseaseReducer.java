package reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utils.OWLRuleChainUtil;
import utils.Triple;
import utils.TripleTool;


public class GeneDiseaseReducer extends Reducer<Text, Text, NullWritable, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			GeneDiseaseReducer.Context context) throws IOException,
			InterruptedException {
	
		List<String[]> mapS = new ArrayList<String[]>();
		List<String[]> mapO = new ArrayList<String[]>();
		
		List<List<String[]>> keyMapper = new ArrayList<List<String[]>>();
		keyMapper.add(mapS);
		keyMapper.add(mapO);
		
		
		
		for (Text val : values) {
			Triple tmpT = TripleTool.parseLineToTriple(val.toString());
			int index = OWLRuleChainUtil.getIndexByPredicate(tmpT
					.getPredicate());
			if(index==-1)
				return;
			int current = index % 2;
			int other = (index + 1) % 2;
			if((index+1)==OWLRuleChainUtil.getPredicateNum()&&current==0){
				context.write(NullWritable.get(), val);
				return;
			}
			String otherProp = (current == 0) ? OWLRuleChainUtil
					.getPredicateByIndex(index + 1) : OWLRuleChainUtil
					.getPredicateByIndex(index - 1);
			String currentProp = tmpT.getPredicate();
			String[] keyPart = { tmpT.getSubject(), tmpT.getObject() };

			String[] tmpSArr={keyPart[other]+currentProp,val.toString()};
			keyMapper.get(current).add(tmpSArr);

			List<String> otherPreLst=new ArrayList<String>();
			for(String[] strA:keyMapper.get(other)){
				if(strA[0].equals(keyPart[other] + otherProp)){
					otherPreLst.add(strA[1]);
				}
				
			}

			if(otherPreLst.size()==0)
				continue;
			
			for(String otherTriple:otherPreLst){
				Triple joinValue=TripleTool.parseLineToTriple(otherTriple);
				Triple[] vList = { tmpT, joinValue };
				String outputVal = vList[current].getSubject() + "\t"
						+ vList[current].getPredicate() + "$$"
						+ vList[other].getPredicate() + "\t"
						+ vList[other].getObject();
				if(OWLRuleChainUtil.proLst.contains(outputVal)){
					continue;
				}
				OWLRuleChainUtil.proLst.add(outputVal);
				context.write(NullWritable.get(), new Text(outputVal));
			}
		}

	}

}
