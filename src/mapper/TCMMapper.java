package mapper;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utils.Triple;
import utils.TripleTool;
import utils.TriplesUtils;

public class TCMMapper extends Mapper<LongWritable, Text, Text, Triple> {
	
	protected Text oKey = new Text();
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		Triple triple = TripleTool.parseLineToTriple(value.toString());
		String predicate = triple.getPredicate();
		
		if(TriplesUtils.objectKeyPredicateSet.contains(predicate)){
			oKey.set(triple.getObject());
			context.write(oKey, triple);
		}

		if(TriplesUtils.subjectKeyPredicateSet.contains(predicate)){
			oKey.set(triple.getSubject());
			context.write(oKey, triple);
		}
			
	}
}
