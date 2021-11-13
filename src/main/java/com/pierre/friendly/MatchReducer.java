package com.pierre.friendly;

import com.pierre.friendly.Writables.CoupleWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MatchReducer extends Reducer<CoupleWritable, BooleanWritable, Text, IntWritable> {
	@Override
	public void reduce(CoupleWritable key, Iterable<BooleanWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		if(key.first != -1){
			for(BooleanWritable value : values) {
				if(!value.get()) { //a False means "we can't be recommended since we're already friends"
					return;
				} else { //a True means "we have one (more) common friend"
					sum++;
				}
			}
		} //else: key.first is -1, it's just a fake connection - just let the number of common connections stay at 0 and print it directly

		context.write(new Text(key.toText()), new IntWritable(sum));
		//prints a result only if one of the people is -1, or if both could actually be recommended to each other
	}
}
