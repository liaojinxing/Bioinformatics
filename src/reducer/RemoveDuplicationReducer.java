package reducer;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RemoveDuplicationReducer extends Reducer<Text, NullWritable, NullWritable, Text> {

	@Override
	protected void reduce(Text key, Iterable<NullWritable> valueLst,
			RemoveDuplicationReducer.Context context)
			throws IOException, InterruptedException {
		
		context.write(NullWritable.get(), key);
	}
}