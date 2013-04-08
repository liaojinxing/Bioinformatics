package reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utils.Triple;
import utils.TriplesUtils;

public class TCMReducer extends Reducer<Text, Triple, Text, Triple> {
	
	public void reduce(Text key, Iterable<Triple> values,
			Context context) throws IOException, InterruptedException{
		Set<String> subclassURIs = new HashSet<String>();
		Set<String> typeURIs = new HashSet<String>();
		Triple oTriple = new Triple();
		
		for(Triple triple : values) {
			if (triple.getPredicate().equals(TriplesUtils.S_RDFS_SUBCLASS)) {
				subclassURIs.add(triple.getObject());
			} else if (triple.getPredicate().equals(TriplesUtils.S_RDF_TYPE)){
				typeURIs.add(triple.getSubject());
			}
		}
		
		Iterator<String> itr = typeURIs.iterator();
		oTriple.setPredicate(TriplesUtils.S_RDF_TYPE);
		oTriple.setObjectLiteral(false);
		while (itr.hasNext()) {
			oTriple.setSubject(itr.next());
			Iterator<String> itrSubclass = subclassURIs.iterator();
			while (itrSubclass.hasNext()) {
				oTriple.setObject(itrSubclass.next());
				context.write(new Text(oTriple.getSubject()), oTriple);
				System.out.println(oTriple.toString());
			}
		}
		
	}
}
