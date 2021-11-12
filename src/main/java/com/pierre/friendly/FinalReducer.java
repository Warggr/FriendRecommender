package com.pierre.friendly;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FinalReducer extends Reducer<CoupleWritable, BooleanWritable, CoupleWritable, IntWritable>{
	@Override
	protected void reduce(CoupleWritable key, Iterable<BooleanWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
	    for(BooleanWritable value : values) {
			if(!value.get()) {
				sum = 0;
				break;
			} else {
				sum++;
			}
		}

	    context.write(key, new IntWritable(sum));
	}
}
