package com.pierre.friendly;

import com.pierre.friendly.Writables.CoupleWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.LinkedList;

public class MatchMakerMapper extends Mapper<LongWritable, Text, CoupleWritable, BooleanWritable> {
	
    private final static BooleanWritable BWtrue = new BooleanWritable(true);
    private final static BooleanWritable BWfalse = new BooleanWritable(false);
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split("\t");
		long me = Long.parseLong(line[0]);
		//the fictional user -1 is a control channel. Every user will state that he "could be recommended to -1". This will not be processed normally,
		//but will be kept in the stream until the end - to make sure that every user comes up in the final result set.
		context.write(new CoupleWritable(-1L, me), BWtrue);
		
		if(line.length == 1) return; //If the user had no friends, just stop here.
		
		String[] friends = line[1].split(",");

		LinkedList<Long> otherFriends = new LinkedList<Long>();

		for(String friend : friends) {
			long you = Long.parseLong(friend);
			if(you < me){ //ensuring we don't print this twice - it is printed only by me when I'm greater, not by you when you're smaller
				context.write(new CoupleWritable(me, you), BWfalse); //"I can't be recommended to you" (since we're already friends)
			}
			for(long otherFriendId : otherFriends) {
				context.write(new CoupleWritable(you, otherFriendId), BWtrue); //"you have one common connection with him"
			}
			otherFriends.add(you);
		}
	}
}
