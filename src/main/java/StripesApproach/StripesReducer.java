package StripesApproach;

import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class StripesReducer extends
		Reducer<Text, NeighbourOccuranceMap, Text, NeighbourOccuranceMap> {

	private NeighbourOccuranceMap aggrOccurace = new NeighbourOccuranceMap();

	public void reduce(
			Text word,
			java.lang.Iterable<NeighbourOccuranceMap> occuranceMaps,
			org.apache.hadoop.mapreduce.Reducer<Text, NeighbourOccuranceMap, Text, NeighbourOccuranceMap>.Context context)
			throws java.io.IOException, InterruptedException {

		aggrOccurace.clear();
		for (NeighbourOccuranceMap occuranceMap : occuranceMaps) {
			Set<Writable> keys = occuranceMap.keySet();
			for (Writable key : keys) {

				if (aggrOccurace.containsKey(key)) {
					IntWritable count = (IntWritable) aggrOccurace
							.get(key);
					count.set(count.get() + 1);
				} else {
					aggrOccurace.put(key, new IntWritable(1));
				}
			}
		}
		
		System.out.println("--------------REDUCER OUTPUTS---------------");
		System.out.println(word+ " "+ aggrOccurace);
		context.write(word, aggrOccurace);

	}

}
