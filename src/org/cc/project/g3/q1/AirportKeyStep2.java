package org.cc.project.g3.q1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirportKeyStep2 implements WritableComparable<AirportKeyStep2> {
    Text airportCode;
    IntWritable frequency;
    IntWritable rank;

    public void setRank(IntWritable rank) {
        this.rank = rank;
    }

    @SuppressWarnings("unused")
    public AirportKeyStep2() {
        this.airportCode = new Text();
        this.frequency = new IntWritable(0);
        this.rank = new IntWritable(0);
    }

    public AirportKeyStep2(Text airportCode, IntWritable frequency) {
        this.airportCode = airportCode;
        this.frequency = frequency;
        this.rank = new IntWritable(0);
    }

    @Override
    public int compareTo(AirportKeyStep2 o) {
        if (null == o) {
            return 0;
        }
        // Reverse sort by value
        int cmp = (-1) * frequency.compareTo(o.frequency);
        if (cmp != 0) {
            return cmp;
        }
        return airportCode.compareTo(o.airportCode);
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.airportCode.write(dataOutput);
        this.frequency.write(dataOutput);
        this.rank.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.airportCode.readFields(dataInput);
        this.frequency.readFields(dataInput);
        this.rank.readFields(dataInput);
    }

    @Override
    public String toString() {
        return rank + "," + frequency;
    }
}
