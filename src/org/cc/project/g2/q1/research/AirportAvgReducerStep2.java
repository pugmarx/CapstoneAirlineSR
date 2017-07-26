package org.cc.project.g2.q1.research;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AirportAvgReducerStep2 extends Reducer<Text, NullWritable, Text, NullWritable> {

    // key: CMI,AA,1.05
    // key: ATL,9E,2.03
    public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException,
            InterruptedException {
        context.write(key, NullWritable.get());
    }
}
