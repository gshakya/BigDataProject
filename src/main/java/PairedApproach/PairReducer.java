package PairedApproach;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class PairReducer extends
		Reducer<WordPair, IntWritable, WordPair, IntWritable> {
	private IntWritable totalCount = new IntWritable();

	@Override
	protected void reduce(WordPair key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		System.out.println("-----Entered Reducer----------");

		int count = 0;
		for (IntWritable value : values) {
			count += value.get();
		}
		
		if (key.getSecondElement().toString().equals("*"))
		System.out.println(key + "," + count);

		totalCount.set(count);
		context.write(key, totalCount);
	}

}
