package com.pierre.friendly;

import com.pierre.friendly.Writables.RecommWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.PriorityQueue;

public class FinalReducer extends Reducer<LongWritable, RecommWritable, LongWritable, Text>{
	@Override
	protected void reduce(LongWritable key, Iterable<RecommWritable> values, Context context)
			throws IOException, InterruptedException {
		PriorityQueue<RecommWritable> queue = new PriorityQueue<>();
		//RecommWritables are sorted by their b-value (number of common friends) first, then by their a-value (ID of the person)
		//We sort them using a priority queue.
	    for(RecommWritable value : values) {
			queue.add(value.clone());
		}

		StringBuilder ret = new StringBuilder();
		for(int i = 0; i<10; i++) {
			RecommWritable couple = queue.poll();

			assert couple != null;
			//we know exactly what the last member of the queue is: it is the connection with -1 (0 common friends).
			if(couple.personId == -1) break;//Ignore that last one.
			
			if(i != 0) ret.append(',');
			ret.append(couple.personId);
		}
		context.write(key, new Text(ret.toString()));
	}
}
