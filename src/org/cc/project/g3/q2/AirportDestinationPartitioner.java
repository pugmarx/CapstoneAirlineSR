package org.cc.project.g3.q2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by Hobbes on 24/07/17.
 */
public class AirportDestinationPartitioner extends Partitioner<Text, NullWritable> {

    @Override
    // Partitioning by Origin -- all flights from the same origin end-up at the same partition
    public int getPartition(Text text, NullWritable nullWritable, int i) {
        String[] sArr = text.toString().split(",");
        return (Math.abs((sArr[0] + sArr[1]).hashCode()) * 127) % i;
    }
}