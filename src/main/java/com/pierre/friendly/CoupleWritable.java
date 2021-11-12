package com.pierre.friendly;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CoupleWritable implements WritableComparable<CoupleWritable> {
    private int[] peopleId = new int[2];

    public CoupleWritable() {}

    public CoupleWritable(int alice, int bob) {
        peopleId[0] = alice;
        peopleId[1] = bob;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        for (int person : peopleId) out.writeInt(person);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        peopleId = new int[2];
        for(int i=0; i<2; i++) {
            peopleId[i] = in.readInt();
        }
    }

    public static CoupleWritable read(DataInput in) throws IOException {
        CoupleWritable c = new CoupleWritable();
        c.readFields(in);
        return c;
    }

    @Override
    public int compareTo(CoupleWritable other) {
        int d = Integer.compare(this.peopleId[0], other.peopleId[0]);
        if(d == 0) return Integer.compare(this.peopleId[1], other.peopleId[1]);
        else return d;
    }
}
