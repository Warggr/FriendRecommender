package com.pierre.friendly;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Writables {
    private static abstract class PairWritable<A, B> {
        public A a;
        public B b;

        public PairWritable() {}

        public PairWritable(A alice, B bob) { a = alice; b = bob; }
    }

    public static class RecommWritable extends PairWritable<Long, Integer> implements WritableComparable<RecommWritable> {
        RecommWritable() {}
        RecommWritable(Long a, Integer b) { super(a, b); }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(a);
            out.writeInt(b);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            a = in.readLong();
            b = in.readInt();
        }

        @Override
        public int compareTo(RecommWritable other) {
            int d = Integer.compare(this.b, other.b);
            if(d == 0) return Long.compare(this.a, other.a);
            else return d;
        }
    }

    public static class CoupleWritable extends PairWritable<Long, Long> implements WritableComparable<CoupleWritable> {
        CoupleWritable() {}
        CoupleWritable(Long a, Long b) { super(a, b); }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(a);
            out.writeLong(b);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            a = in.readLong();
            b = in.readLong();
        }

        @Override
        public int compareTo(CoupleWritable other) {
            int d = Long.compare(this.b, other.b);
            if(d == 0) return Long.compare(this.a, other.a);
            else return d;
        }
    }
}
