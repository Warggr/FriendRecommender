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
	    for(RecommWritable value : values)
			if(value.b != 0)
				queue.add(value);

		int i = 0;
		StringBuilder ret = new StringBuilder();
		for(RecommWritable couple : queue) {
			ret.append(couple.a).append(',');
			i++;
			if(i == 10) break;
		}
		context.write(key, new Text(ret.toString()));
	}
}
