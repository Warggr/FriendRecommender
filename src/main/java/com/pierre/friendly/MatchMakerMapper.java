package com.pierre.friendly;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.LinkedList;

public class MatchMakerMapper extends Mapper<LongWritable, Text, CoupleWritable, BooleanWritable>{
	
    private final static BooleanWritable BWtrue = new BooleanWritable(true);
    private final static BooleanWritable BWfalse = new BooleanWritable(false);
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	System.out.println(value.toString());
		String[] line = value.toString().split("\t");
		for(String subline : line){
			System.out.println("   " + subline);
		}
		String[] friends = line[1].split(",");
		int me = Integer.parseInt(line[0]);

		LinkedList<Integer> otherFriends = new LinkedList<Integer>();

		for(String friend : friends) {
			int you = Integer.parseInt(friend);
			context.write(new CoupleWritable(me, you), BWfalse);
			for(Integer otherFriendId : otherFriends) {
				context.write(new CoupleWritable(you, otherFriendId), BWtrue);
			}
			otherFriends.add(you);
		}
	}
}
