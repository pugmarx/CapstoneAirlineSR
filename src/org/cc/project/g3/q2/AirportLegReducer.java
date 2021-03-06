package org.cc.project.g3.q2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AirportLegReducer extends Reducer<AirportDestinationKey, IntWritable, AirportDestinationKey,
        NullWritable> {

    int nCount = 0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        nCount = 0;
    }

    public void reduce(AirportDestinationKey key, Iterable<IntWritable> values, Context context) throws IOException,
            InterruptedException {
        if (nCount == 0) {
            context.write(key, NullWritable.get());
            nCount++;
        }
    }
}