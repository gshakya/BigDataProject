package StripesApproach;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class NeighbourOccuranceMap extends MapWritable{
	
	@Override
	public String toString() {
		// 
		String output= "{";
		for(Entry<Writable, Writable> e : this.entrySet()){
			output += e.getKey()+"-->"+e.getValue()+" "; 
		}
		output+="}";
		return output;
	}
}