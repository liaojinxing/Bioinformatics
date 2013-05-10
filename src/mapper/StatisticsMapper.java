package mapper;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StatisticsMapper extends Mapper<Object, Text, Text, NullWritable>{

	@Override
	protected void map(Object key, Text value,
			RemoveDuplicationMapper.Context context)
			throws IOException, InterruptedException {
		context.write(new Text(key.toString()), NullWritable.get());
	}

}

