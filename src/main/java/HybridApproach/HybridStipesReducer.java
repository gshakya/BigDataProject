package HybridApproach;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class HybridStipesReducer extends
		Reducer<HybridWordPair, IntWritable, Text, HybridNeighbourOccuranceMap> {

	private HybridNeighbourOccuranceMap relFreqMap = new HybridNeighbourOccuranceMap();

	private MapWritable countMaps = new MapWritable();

	private String previousEle = "";

	private int total = 0;

	@Override
	protected void reduce(HybridWordPair key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		 System.out.println("-----Entered Reducer----------");

		String firstEle = key.getFirstElement().toString();
		String secondEle = key.getSecondElement().toString();

		int count = 0;

		for (IntWritable value : values) {
			count += value.get();
		}

		if (!previousEle.equals(firstEle) && !previousEle.equals("")) {
			
//			System.out.println(countMaps.size());
			Writable totalval= countMaps.get(new Text("*"));
			total = Integer.parseInt(totalval.toString());
			for (Entry<Writable, Writable> countMap : countMaps.entrySet()) {
				Writable ele = countMap.getKey();
				IntWritable value = (IntWritable) countMap.getValue();

				if (!ele.toString().equals("*")) {
					System.out.println(ele + " " + value.get() + " " + total);

					relFreqMap
							.put(ele, new DoubleWritable((double)value.get() /(double) total));
				}
			}

			System.out.println(previousEle + " " + relFreqMap);
			context.write(new Text(previousEle), relFreqMap);
			countMaps.clear();
			relFreqMap.clear();
			// total = 0;
		}

		IntWritable newValue = countMaps.get(secondEle) == null ? new IntWritable(
				count) : new IntWritable(
				((IntWritable) countMaps.get(secondEle)).get() + count);
				
		System.out.println(secondEle+" "+ newValue);
		countMaps.put(new Text(secondEle), newValue);
		previousEle = firstEle;
//		System.out.println(key + "," + count);

	}

	@Override
	protected void cleanup(
			Reducer<HybridWordPair, IntWritable, Text, HybridNeighbourOccuranceMap>.Context context)
			throws IOException, InterruptedException {
//		System.out.println(countMaps.size());
		Writable totalval= countMaps.get(new Text("*"));
		total = Integer.parseInt(totalval.toString());
		for (Entry<Writable, Writable> countMap : countMaps.entrySet()) {
			Writable ele = countMap.getKey();
			IntWritable value = (IntWritable) countMap.getValue();

			if (!ele.toString().equals("*")) {
				System.out.println(ele + " " + value.get() + " " + total);

				relFreqMap
						.put(ele, new DoubleWritable((double)value.get() /(double) total));
			}
		}

		System.out.println(previousEle + " " + relFreqMap);
		context.write(new Text(previousEle), relFreqMap);

		relFreqMap.clear();
		// total = 0;
	}
}
