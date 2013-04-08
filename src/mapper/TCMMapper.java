package mapper;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utils.Triple;
import utils.TriplesUtils;

public class TCMMapper extends Mapper<LongWritable, Text, Text, Triple> {
	
	protected Text oKey = new Text();
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String line = value.toString();
		String[] resources = line.split("\t");
		String subject = resources[0];
		String predicate = resources[1];
		String object = resources[2];
		Triple triple = new Triple(subject,predicate,object, false);
		
		if (predicate.equals(TriplesUtils.S_RDF_TYPE)) {
			oKey.set(object);
			context.write(oKey, triple);
		}
		
		if (predicate.equals(TriplesUtils.S_RDFS_SUBCLASS)) {
			oKey.set(subject);
			context.write(oKey, triple);
		}
			
	}
}
