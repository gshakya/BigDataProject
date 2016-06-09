package PairedApproach;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PairMapper extends Mapper<LongWritable, Text, WordPair, IntWritable> {
	private WordPair wordPair = new WordPair();
	private WordPair asterixPair = new WordPair();
	private IntWritable one = new IntWritable(1);

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] words = value.toString().split(" ");
		// System.out.println("Getting Neighbours");
		for (int i = 0; i < words.length; i++) {
			// System.out.println(words[i]);

			wordPair.setFirstElement(new Text(words[i]));
			asterixPair.setFirstElement(new Text(words[i]) );
			asterixPair.setSecondElement(new Text("*") );
			int totalCount=0;
			
			ArrayList<String> neighbours = getNeighbours(i, words);
			for (String neighbour : neighbours) {
				totalCount++;
				wordPair.setSecondElement(new Text(neighbour));
				System.out.println(wordPair);
				context.write(wordPair, one);
				
			}
			context.write(asterixPair,new IntWritable(totalCount));
			neighbours = null;
		}

	}

	private ArrayList<String> getNeighbours(int ofItem, String[] appliedOn) {
		ArrayList<String> neighbours = new ArrayList<String>();
		int index = ofItem + 1;
		while (index < appliedOn.length
				&& !appliedOn[index].equals(appliedOn[ofItem])) {
			neighbours.add(appliedOn[index]);
			index++;
		}

		return neighbours;

	}

}
