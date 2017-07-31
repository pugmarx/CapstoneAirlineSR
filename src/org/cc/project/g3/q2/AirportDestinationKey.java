package org.cc.project.g3.q2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Hobbes on 24/07/17.
 */
public class AirportDestinationKey implements WritableComparable<AirportDestinationKey> {

    private Text origin;
    private Text destination;
    //private Double meanArrDelay;

//    public void setMeanArrDelay(Double meanArrDelay) {
//        this.meanArrDelay = meanArrDelay;
//    }

    public AirportDestinationKey(Text origin, Text destination) {
        this.origin = origin;
        this.destination = destination;
       // this.meanArrDelay = 0.00d;
    }

    @SuppressWarnings("unused")
    // required for implicit call
    public AirportDestinationKey() {
        this.origin = new Text();
        this.destination = new Text();
        //this.meanArrDelay = 0.00d;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.origin.write(dataOutput);
        this.destination.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.origin.readFields(dataInput);
        this.destination.readFields(dataInput);
    }

    @Override
    public String toString() {
        return origin + "," + destination;
    }

    @Override
    public int compareTo(AirportDestinationKey o) {
        if (null == o) {
            return 0;
        }
        int intOrigin = origin.compareTo(o.origin);
        if (intOrigin != 0) {
            return intOrigin;
        } else {
            return destination.compareTo(o.destination);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AirportDestinationKey that = (AirportDestinationKey) o;

        if (origin != null ? !origin.equals(that.origin) : that.origin != null) return false;
        return destination != null ? destination.equals(that.destination) : that.destination == null;
    }

    @Override
    public int hashCode() {
        int result = origin != null ? origin.hashCode() : 0;
        result = 31 * result + (destination != null ? destination.hashCode() : 0);
        return result;
    }
}
