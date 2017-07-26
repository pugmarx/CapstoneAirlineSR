package org.cc.project.g2.q1.research;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by Hobbes on 24/07/17.
 */
public class AirportAvgGroupComparator extends WritableComparator {

    protected AirportAvgGroupComparator() {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        Text ip1 = (Text) w1;
        Text ip2 = (Text) w2;

        String[] s1Arr = ip1.toString().split(",");
        String[] s2Arr = ip2.toString().split(",");

        // Check if the origin matches
        return s1Arr[0].compareTo(s2Arr[0]);
    }
}
