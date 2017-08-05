package org.cc.project.g2.q2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Hobbes on 24/07/17.
 */
public class AirportDestinationKey implements WritableComparable<AirportDestinationKey> {

    Text origin;
    Text dest;
    DoubleWritable avgDepDelay;

    public AirportDestinationKey(Text origin, Text dest) {
        this.origin = origin;
        this.dest = dest;
        this.avgDepDelay = new DoubleWritable(0);
    }

    @SuppressWarnings("unused")
    // required for implicit call
    public AirportDestinationKey() {
        this.origin = new Text();
        this.dest = new Text();
        this.avgDepDelay = new DoubleWritable(0);
    }

    public void setAvgDepDelay(DoubleWritable avgDepDelay) {
        this.avgDepDelay = avgDepDelay;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.origin.write(dataOutput);
        this.dest.write(dataOutput);
        this.avgDepDelay.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.origin.readFields(dataInput);
        this.dest.readFields(dataInput);
        this.avgDepDelay.readFields(dataInput);
    }

    @Override
    public String toString() {
        return origin + "," + dest + "," + avgDepDelay;
    }

    @Override
    public int compareTo(AirportDestinationKey o) {
        if (null == o) {
            return 0;
        }
        int intOrigin = origin.compareTo(o.origin);
        if (intOrigin != 0) {
            return intOrigin;
        }
        int intDepDel = avgDepDelay.compareTo(o.avgDepDelay);
        if (intDepDel != 0) {
            return intDepDel;
        }

        return dest.compareTo(o.dest);

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AirportDestinationKey that = (AirportDestinationKey) o;

        if (origin != null ? !origin.equals(that.origin) : that.origin != null) return false;
        return dest != null ? dest.equals(that.dest) : that.dest == null;
    }

    @Override
    public int hashCode() {
        int result = origin != null ? origin.hashCode() : 0;
        result = 31 * result + (dest != null ? dest.hashCode() : 0);
        return result;
    }
}
