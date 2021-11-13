package com.pierre.friendly;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Writables {
    public static class RecommWritable implements WritableComparable<RecommWritable>, Cloneable {
        public long personId;
        public int nbConnections;

        RecommWritable() {}
        RecommWritable(Long personId, Integer nbConnections) { this.personId = personId; this.nbConnections = nbConnections; }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(personId);
            out.writeInt(nbConnections);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            personId = in.readLong();
            nbConnections = in.readInt();
        }

        @Override
        public int compareTo(RecommWritable other) {
            //it's inverted on purpose - we want Recommandations with smaller number of connections to have a larger value
            //and be later in the priority queue.
            int d = Integer.compare(other.nbConnections, this.nbConnections);
            if(d == 0) return Long.compare(other.personId, this.personId);
            else return d;
        }

        @Override
        public RecommWritable clone() {
        	return new RecommWritable(personId, nbConnections);
        }
    }

    public static class CoupleWritable implements WritableComparable<CoupleWritable> {
        public static final String SEPARATOR = "x";
        Long first, second;

        CoupleWritable() {}
        CoupleWritable(long a, long b) {
            if(a < b) {
                this.first = a; this.second = b;
            } else {
                this.first = b; this.second = a;
            }
        }

        public static CoupleWritable fromText(String str) {
            String[] nb = str.split(SEPARATOR);
            return new CoupleWritable(Long.parseLong(nb[0]), Long.parseLong(nb[1]));
        }

        public String toText() {
            return first + Writables.CoupleWritable.SEPARATOR + second;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(first);
            out.writeLong(second);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            first = in.readLong();
            second = in.readLong();
        }

        @Override
        public int compareTo(CoupleWritable other) {
            int d = Long.compare(this.first, other.first);
            if(d == 0) return Long.compare(this.second, other.second);
            else return d;
        }
    }
}
