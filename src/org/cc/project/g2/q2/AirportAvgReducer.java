package org.cc.project.g2.q2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

public class AirportAvgReducer extends Reducer<AirportDestinationKey, IntWritable, AirportDestinationKey, NullWritable> {

    private DoubleWritable result = new DoubleWritable();
    //private TreeMap<AirportCarrierKey, NullWritable> avgToAirlineMap = new TreeMap<>();

    public void reduce(AirportDestinationKey key, Iterable<IntWritable> values, Context context) throws IOException,
            InterruptedException {

        double sum = 0.00d;
        int count = 1;

        for (IntWritable val : values) {
            sum += val.get();
            count++;
        }

        double average = sum / count;
        DecimalFormat df = new DecimalFormat("#.##");
        result.set(Double.parseDouble(df.format(average)));
        key.setAvgDepDelay(result);
        //String val = String.format("%s,%s", key, result);
        context.write(key, NullWritable.get());
    }
}
