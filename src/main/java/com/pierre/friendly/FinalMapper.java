package com.pierre.friendly;

import com.pierre.friendly.Writables.CoupleWritable;
import com.pierre.friendly.Writables.RecommWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FinalMapper extends Mapper<LongWritable, Text, LongWritable, RecommWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");

        RecommWritable writable = new RecommWritable(); //writable.a : ID of possible friend; writable.b : number of common connections
        writable.b = Integer.valueOf(parts[1]); //reading the number of connections from input

        CoupleWritable couple = CoupleWritable.fromText(parts[0]); //couple.a = idA; couple.b = idB;

        writable.a = couple.a; //writable= {(id=)a : idA, (connections=)b : nbConnections }
        context.write(new LongWritable(couple.b), writable); // write idB : idA x nbConnections

        writable.a = couple.b;
        context.write(new LongWritable(couple.a), writable); // write idA : idB x nbConnections
    }
}
