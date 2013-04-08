package jobs;

import java.io.IOException;

import mapper.TCMMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import reducer.TCMReducer;
import utils.Triple;


public class TCMReasoner {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (args.length < 1) {
			System.out.println("USAGE: RFDSReasoner [pool path] [options]");
			return;
		}
						
		Job job = new Job(conf, "reasoner");
		job.setJarByClass(TCMReasoner.class);
		System.out.println(args[0]);
		
		job.setMapperClass(TCMMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Triple.class);
				
		job.setReducerClass(TCMReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Triple.class);
				
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
				
		job.waitForCompletion(true);
		Counter derivedTriples = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter","REDUCE_OUTPUT_RECORDS");
		System.out.println(derivedTriples.getValue());

		return ;
	}	
}