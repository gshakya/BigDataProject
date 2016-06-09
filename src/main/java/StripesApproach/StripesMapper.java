package StripesApproach;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StripesMapper extends
		Mapper<LongWritable, Text, Text, NeighbourOccuranceMap> {
	private NeighbourOccuranceMap occuranceMap = new NeighbourOccuranceMap();
	private Text word = new Text();

	@Override
	public void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, NeighbourOccuranceMap>.Context context)
			throws IOException, InterruptedException {
		String[] words = value.toString().split(" ");
		// System.out.println("Getting Neighbours");
		for (int i = 0; i < words.length; i++) {
			// System.out.println(words[i]);
			ArrayList<String> neighbours = getNeighbours(i, words);
			word.set(words[i]);

			for (String neighbour : neighbours) {
				if (occuranceMap.containsKey(neighbour)) {
					IntWritable count = (IntWritable) occuranceMap
							.get(neighbour);
					count.set(count.get() + 1);
				} else {
					occuranceMap.put(new Text(neighbour), new IntWritable(1));
				}

			}
			
			System.out.println("----------MAPPER RESULTS-------------");
			System.out.println(word+" "+ occuranceMap);
			context.write(word, occuranceMap);

			occuranceMap.clear();
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
