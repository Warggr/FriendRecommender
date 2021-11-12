package com.pierre.friendly;

import com.pierre.friendly.Writables.CoupleWritable;
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
		String[] line = value.toString().split("\t");
		if(line.length == 1){ //If the user has no friends
			context.write(new CoupleWritable(key.get(), key.get()), BWfalse); //write a "I'm not friend with myself" so the ID still pops up in the next result set
		} else {
			String[] friends = line[1].split(",");
			long me = Long.parseLong(line[0]);

			LinkedList<Long> otherFriends = new LinkedList<Long>();

			for(String friend : friends) {
				long you = Long.parseLong(friend);
				context.write(new CoupleWritable(me, you), BWfalse);
				System.err.println(you + " and " + me + " are already friends");
				for(Long otherFriendId : otherFriends) {
					System.err.println(you + " and " + otherFriendId + " are friends via " + me);
					context.write(new CoupleWritable(you, otherFriendId), BWtrue);
				}
				otherFriends.add(you);
			}
		}
	}
}
