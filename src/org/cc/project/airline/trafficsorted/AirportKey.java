package org.cc.project.airline.trafficsorted;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirportKey implements WritableComparable {

    private Text airportCode;

    public void setAirportCode(Text airportCode) {
        this.airportCode = airportCode;
    }

    public void setInOutCount(IntWritable inOutCount) {
        this.inOutCount = inOutCount;
    }

    public Text getAirportCode() {
        return airportCode;
    }

    public IntWritable getInOutCount() {
        return inOutCount;
    }

    private IntWritable inOutCount;

    @Override
    public int compareTo(Object o) {
        return (-1) * inOutCount.compareTo(((AirportKey)o).getInOutCount());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
