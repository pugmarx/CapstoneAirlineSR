package org.cc.project.g3.q2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

public class AirportLegReducer extends Reducer<AirportDestinationKey, IntWritable, AirportDestinationKey, NullWritable> {

    private DoubleWritable result = new DoubleWritable();

    public void reduce(AirportDestinationKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}