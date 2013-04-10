package jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import mapper.RemoveDuplicationMapper;
import reducer.RemoveDuplicationReducer;

public class RemoveDuplicationReasoner {
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: OWLTripleMRDriver <in> <out>");
			System.exit(2);
		}
		
		Job job=new Job(conf);
		job.setJarByClass(RemoveDuplicationReasoner.class);
		job.setMapperClass(RemoveDuplicationMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setReducerClass(RemoveDuplicationReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));	
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[1]), true);

		job.waitForCompletion(true);
		//OWLRuleChainUtil.updatePredicateArrIntoFile();
	}
}

