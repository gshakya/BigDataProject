package StripesApproach;

import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class StripesReducer extends
		Reducer<Text, NeighbourOccuranceMap, Text, NeighbourOccuranceMap> {

	private NeighbourOccuranceMap aggrOccurace = new NeighbourOccuranceMap();

	public void reduce(
			Text word,
			Iterable<NeighbourOccuranceMap> occuranceMaps,
			org.apache.hadoop.mapreduce.Reducer<Text, NeighbourOccuranceMap, Text, NeighbourOccuranceMap>.Context context)
			throws java.io.IOException, InterruptedException {
		
		int total= 0;

		aggrOccurace.clear();
		for (NeighbourOccuranceMap occuranceMap : occuranceMaps) {
			
			
			Set<Writable> keys = occuranceMap.keySet();
			for (Writable key : keys) {
				DoubleWritable count = new DoubleWritable(1);
				if (aggrOccurace.containsKey(key)) {
					 count = (DoubleWritable) aggrOccurace
							.get(key);
					count.set(count.get() + 1);
					
					
				} else {
					aggrOccurace.put(key, count);
				}
				total += count.get();
			}
			
			for (Entry occurMap : aggrOccurace.entrySet()){
				Writable key =  (Writable) occurMap.getKey();
				DoubleWritable value = (DoubleWritable) occurMap.getValue();
				aggrOccurace.put(key, new DoubleWritable((double)value.get() / (double)total));
			}
		}
		
		System.out.println("--------------REDUCER OUTPUTS---------------");
		System.out.println(word+ " "+ aggrOccurace);
		context.write(word, aggrOccurace);

	}

}
