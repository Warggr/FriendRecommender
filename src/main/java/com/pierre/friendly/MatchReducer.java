package com.pierre.friendly;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import com.pierre.friendly.Writables.CoupleWritable;

import java.io.IOException;
import java.util.logging.Level;

public class MatchReducer extends Reducer<CoupleWritable, BooleanWritable, Text, IntWritable> {
	@Override
	public void reduce(CoupleWritable key, Iterable<BooleanWritable> values, Context context)
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

		MyLogger.log.log(Level.INFO,key.toText() + " are friends " + sum + " times");
		context.write(new Text(key.toText()), new IntWritable(sum));
	}
}
