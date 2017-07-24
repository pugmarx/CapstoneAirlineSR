package org.cc.project.g2.q1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Hobbes on 24/07/17.
 */
public class AirportCarrierKey implements WritableComparable<AirportCarrierKey> {

    Text origin;
    Text carrier;

    public AirportCarrierKey(Text origin, Text carrier) {
        this.origin = origin;
        this.carrier = carrier;
    }

    public AirportCarrierKey() {
        this.origin = new Text();
        this.carrier = new Text();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.origin.write(dataOutput);
        this.carrier.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.origin.readFields(dataInput);
        this.carrier.readFields(dataInput);
    }

    @Override
    public String toString() {
        return origin + "," + carrier;
    }

    @Override
    public int compareTo(AirportCarrierKey o) {
        if (null == o) {
            return 0;
        }
        int intOrigin = origin.compareTo(o.origin);
        if (intOrigin != 0) {
            return intOrigin;
        } else {
            return carrier.compareTo(o.carrier);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AirportCarrierKey that = (AirportCarrierKey) o;

        if (origin != null ? !origin.equals(that.origin) : that.origin != null) return false;
        return carrier != null ? carrier.equals(that.carrier) : that.carrier == null;
    }

    @Override
    public int hashCode() {
        int result = origin != null ? origin.hashCode() : 0;
        result = 31 * result + (carrier != null ? carrier.hashCode() : 0);
        return result;
    }
}
