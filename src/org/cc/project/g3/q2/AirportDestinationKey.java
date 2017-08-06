package org.cc.project.g3.q2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.cc.project.common.misc.AirlineConstants;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Hobbes on 24/07/17.
 */
public class AirportDestinationKey implements WritableComparable<AirportDestinationKey> {

    private Text origin;
    private Text transit;
    private Text destination;
    private IntWritable totalDelay;
    private Text airline;
    private Text fltNumber;
    private Text depTime;
    private Text depDate;
    private Text leg;
    private Text tripDate;

    public AirportDestinationKey() {
        this.origin = new Text();
        this.transit = new Text();
        this.destination = new Text();
        this.totalDelay = new IntWritable(0);
        this.airline = new Text();
        this.fltNumber = new Text();
        this.depTime = new Text();
        this.depDate = new Text();
        this.leg = new Text();
        this.tripDate = new Text();

    }

    public AirportDestinationKey(Text origin, Text transit, Text destination, IntWritable totalDelay, Text airline,
                                 Text fltNumber, Text depTime, Text depDate, Text leg, Text tripDate) {
        this.origin = origin;
        this.transit = transit;
        this.destination = destination;
        this.totalDelay = totalDelay;
        this.airline = airline;
        this.fltNumber = fltNumber;
        this.depTime = depTime;
        this.depDate = depDate;
        this.leg = leg;
        this.tripDate = tripDate;
    }


    @Override
    public int compareTo(AirportDestinationKey o) {

        if (null == o) {
            return 0;
        }
        int cmp = 0;

        if (leg.toString().equals("leg1")) {
            cmp = origin.compareTo(o.origin);
            if (cmp != 0) {
                return cmp;
            }
            cmp = transit.compareTo(o.transit);
            if (cmp != 0) {
                return cmp;
            }
        } else {
            cmp = transit.compareTo(o.transit);
            if (cmp != 0) {
                return cmp;
            }
            cmp = destination.compareTo(o.destination);
            if (cmp != 0) {
                return cmp;
            }
        }
        cmp = totalDelay.compareTo(o.totalDelay);
        if (cmp != 0) {
            return cmp;
        }
        cmp = airline.compareTo(o.airline);
        if (cmp != 0) {
            return cmp;
        }
        return fltNumber.compareTo(o.fltNumber);

    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.origin.write(dataOutput);
        this.transit.write(dataOutput);
        this.destination.write(dataOutput);
        this.totalDelay.write(dataOutput);
        this.airline.write(dataOutput);
        this.fltNumber.write(dataOutput);
        this.depTime.write(dataOutput);
        this.depDate.write(dataOutput);
        this.leg.write(dataOutput);
        this.tripDate.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.origin.readFields(dataInput);
        this.transit.readFields(dataInput);
        this.destination.readFields(dataInput);
        this.totalDelay.readFields(dataInput);
        this.airline.readFields(dataInput);
        this.fltNumber.readFields(dataInput);
        this.depTime.readFields(dataInput);
        this.depDate.readFields(dataInput);
        this.leg.readFields(dataInput);
        this.tripDate.readFields(dataInput);
    }

    @Override
    public String toString() {
        return origin +
                "," + transit +
                "," + destination +
                "," + tripDate +
                "," + leg +
                "," + totalDelay +
                "," + airline +
                "," + fltNumber +
                "," + depDate +
                "," + depTime;

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AirportDestinationKey that = (AirportDestinationKey) o;

        if (origin != null ? !origin.equals(that.origin) : that.origin != null) return false;
        if (transit != null ? !transit.equals(that.origin) : that.transit != null) return false;
        if (destination != null ? !destination.equals(that.destination) : that.destination != null) return false;
        if (airline != null ? !airline.equals(that.airline) : that.airline != null) return false;
        if (fltNumber != null ? !fltNumber.equals(that.fltNumber) : that.fltNumber != null) return false;
        return depDate != null ? depDate.equals(that.depDate) : that.depDate == null;
    }

    @Override
    public int hashCode() {
        int result = origin != null ? origin.hashCode() : 0;
        result = 31 * result + (transit != null ? transit.hashCode() : 0);
        result = 31 * result + (destination != null ? destination.hashCode() : 0);
        result = 31 * result + (airline != null ? airline.hashCode() : 0);
        result = 31 * result + (fltNumber != null ? fltNumber.hashCode() : 0);
        result = 31 * result + (depDate != null ? depDate.hashCode() : 0);
        return result;
    }

}
