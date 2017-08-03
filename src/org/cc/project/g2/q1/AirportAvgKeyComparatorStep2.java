package org.cc.project.g2.q1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by Hobbes on 24/07/17.
 * Carries out SecondarySort
 */
public class AirportAvgKeyComparatorStep2 extends WritableComparator {

    protected AirportAvgKeyComparatorStep2() {
        super(Text.class, true);
    }

    // key1: ORD,9E     5.23
    // key2: ORD,UA     1.21
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        Text ip1 = (Text) w1;
        Text ip2 = (Text) w2;

        // [ORD][9E][5.23]
        // [ORD][UA][1.21]
        String[] s1Arr = ip1.toString().split(",");
        String[] s2Arr = ip2.toString().split(",");

        // Check if the origin matches
        int cmp = s1Arr[0].compareTo(s2Arr[0]);
        if (cmp != 0) {
            return cmp;
        }

        // Sort by avg delay
        cmp = Double.compare(Double.valueOf(s1Arr[2]), Double.valueOf(s2Arr[2]));
        if (cmp != 0) {
            return cmp;
        }

        return s1Arr[1].compareTo(s2Arr[1]);
    }

}
