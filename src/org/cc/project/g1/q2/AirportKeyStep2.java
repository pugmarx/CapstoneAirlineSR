package org.cc.project.g1.q2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirportKeyStep2 implements WritableComparable<AirportKeyStep2> {
    Text airportCode;
    DoubleWritable arrivalDelay;

    @SuppressWarnings("unused")
    public AirportKeyStep2() {
        this.airportCode = new Text();
        this.arrivalDelay = new DoubleWritable(0.00);
    }

    public AirportKeyStep2(Text airportCode, DoubleWritable arrivalDelay) {
        this.airportCode = airportCode;
        this.arrivalDelay = arrivalDelay;
    }

    @Override
    public int compareTo(AirportKeyStep2 o) {
        if (null == o) {
            return 0;
        }
        // Reverse sort by value
        int cmp = (-1) * arrivalDelay.compareTo(o.arrivalDelay);
        if (cmp != 0) {
            return cmp;
        }
        return airportCode.compareTo(o.airportCode);
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.airportCode.write(dataOutput);
        this.arrivalDelay.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.airportCode.readFields(dataInput);
        this.arrivalDelay.readFields(dataInput);
    }

    @Override
    public String toString() {
        return airportCode + "," + arrivalDelay;
    }
}
