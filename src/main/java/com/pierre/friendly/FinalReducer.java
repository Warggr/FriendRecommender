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
	    for(RecommWritable value : values) {
			context.write(key, new Text(value.a + " " + value.b));
			if (value.b != 0){
				queue.add(value);
				context.write(new LongWritable(405), new Text(value.a + " " + value.b));
			}
		}

		StringBuilder ret = new StringBuilder();
		for(int i = 0; i<10; i++) {
			RecommWritable couple = queue.poll();
			if(couple == null) break;
			context.write(new LongWritable(404), new Text(couple.a + " " + couple.b));
			if(i != 0) ret.append(',');
			ret.append(couple.a);
		}
		context.write(key, new Text(ret.toString()));
	}
}
