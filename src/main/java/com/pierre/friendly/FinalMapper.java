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
        writable.nbConnections = Integer.parseInt(parts[1]);

        CoupleWritable couple = CoupleWritable.fromText(parts[0]); //couple.a = idA; couple.b = idB;

        writable.personId = couple.first;
        context.write(new LongWritable(couple.second), writable); // write idSecond : idFirst x nbConnections

        if(couple.first != -1){ //an "idA <-> -1" connection needs only to be transmitted for idA,
            // -1 doesn't care how many friends it has
            writable.personId = couple.second;
            context.write(new LongWritable(couple.first), writable); // write idFirst : idSecond x nbConnections
        }
    }
}
