package HybridApproach;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class HybridNeighbourOccuranceMap extends MapWritable{
	
	@Override
	public String toString() {
		// 
		String output= "{";
		for(Entry<Writable, Writable> e : this.entrySet()){
			output += e.getKey()+"-->"+e.getValue().toString()+" "; 
		}
		output+="}";
		return output;
	}
}