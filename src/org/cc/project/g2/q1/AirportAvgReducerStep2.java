package org.cc.project.g2.q1;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AirportAvgReducerStep2 extends Reducer<Text, NullWritable, Text, NullWritable> {

    // key: 1.04  value (could be): CMI,US OR SFO,9E
    // key: 4.4 value: PA
    public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
