package jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import mapper.StatisticsMapper;

public class StatisticsJob {
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: OWLTripleMRDriver <in> <out>");
			System.exit(2);
		}
		
		Job job=new Job(conf);
		job.setJarByClass(StatisticsJob.class);
		job.setMapperClass(StatisticsMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));	
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[1]), true);

		job.waitForCompletion(true);
		
		Counter derivedTriples = job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter","REDUCE_OUTPUT_RECORDS");
		System.out.println(derivedTriples.getValue());
	}
}

