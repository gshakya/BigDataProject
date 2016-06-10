package PairedApproach;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class PairReducer extends
		Reducer<WordPair, IntWritable, WordPair, DoubleWritable> {
	private int total =1;

	@Override
	protected void reduce(WordPair key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		System.out.println("-----Entered Reducer----------");

		int count = 0;
		for (IntWritable value : values) {
			count += value.get();
		}
		
		if (key.getSecondElement().toString().equals("*"))
			total = count;
		 

		
		context.write(key, new DoubleWritable((double)count/(double)total));
	}

}
