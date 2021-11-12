package com.pierre.friendly;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import com.pierre.friendly.Writables.RecommWritable;
import com.pierre.friendly.Writables.CoupleWritable;

import java.io.IOException;

public class FinalMapper extends Mapper<CoupleWritable, IntWritable, LongWritable, RecommWritable> {
    @Override
    protected void map(CoupleWritable key, IntWritable value, Context context)
            throws IOException, InterruptedException {
        RecommWritable writable = new RecommWritable();
        writable.b = value.get();

        writable.a = key.a;
        context.write(new LongWritable(key.b), writable);

        writable.a = key.b;
        context.write(new LongWritable(key.a), writable);
    }
}
