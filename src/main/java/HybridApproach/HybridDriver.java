package HybridApproach;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class HybridDriver {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		Job job;
		try {
			job = Job.getInstance(conf);

			job.setJarByClass(HybridDriver.class);

			FileSystem fs = FileSystem.get(conf);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(HybridNeighbourOccuranceMap.class);
			job.setMapOutputKeyClass(HybridWordPair.class);
			job.setMapOutputValueClass(IntWritable.class);

			job.setMapperClass(HybridPairMapper.class);
			job.setReducerClass(HybridStipesReducer.class);
			
			fs.delete(new Path("output"));
			FileInputFormat.addInputPath(job, new Path("input/pair_input.txt"));
			FileOutputFormat.setOutputPath(job, new Path("output"));

//			fs.delete(new Path(args[1]));
//			FileInputFormat.addInputPath(job, new Path(args[0]));
//			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			job.waitForCompletion(true);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}