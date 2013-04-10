package jobs;

import java.io.IOException;

import mapper.GeneDiseaseMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import reducer.GeneDiseaseReducer;

import utils.OWLRuleChainUtil;


public class GeneDiseaseReasoner {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: GeneDiseaseReasoner <in> <out>");
			System.exit(2);
		}

		Job job = null;
		int iterationNum=0;
		int predicateNum=OWLRuleChainUtil.getPredicateNum();
		while(predicateNum>1){
			job = new Job(conf, "owl rule chain!");
			
			job.setJarByClass(GeneDiseaseReasoner.class);
			job.setMapperClass(GeneDiseaseMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setReducerClass(GeneDiseaseReducer.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);

			String inputPath="";
			if(iterationNum==0){
				inputPath=otherArgs[0];
			}else{
				inputPath="outputowl"+(iterationNum-1);
			}
			FileInputFormat.setInputPaths(job, new Path(inputPath));

			String outputPath="";
			if(predicateNum==2){
				outputPath=otherArgs[1];
			}else{
				outputPath="outputowl"+iterationNum++;
			}
			FileOutputFormat.setOutputPath(job, new Path(outputPath));

			FileSystem.get(job.getConfiguration()).delete(new Path(outputPath), true);
			job.waitForCompletion(true);
			predicateNum=OWLRuleChainUtil.updatePredicateArr();
			System.out.println(predicateNum);
			System.out.println(OWLRuleChainUtil.getPredicateByIndex(0));
		}

	}

}
