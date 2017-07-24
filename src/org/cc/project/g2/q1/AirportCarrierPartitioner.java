package org.cc.project.g2.q1;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by Hobbes on 24/07/17.
 */
public class AirportCarrierPartitioner extends Partitioner<Text, NullWritable> {

    @Override
    public int getPartition(Text text, NullWritable nullWritable, int i) {
        String[] sArr = text.toString().split(",");
        return (Math.abs(sArr[0].hashCode()) * 127) % i;
    }
}