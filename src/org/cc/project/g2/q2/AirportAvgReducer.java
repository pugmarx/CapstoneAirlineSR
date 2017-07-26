package org.cc.project.g2.q2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

public class AirportAvgReducer extends Reducer<AirportDestinationKey, IntWritable, AirportDestinationKey, DoubleWritable> {

    private DoubleWritable result = new DoubleWritable();

    public void reduce(AirportDestinationKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {


        double sum = 0.00d;
        int count = 1;
        //System.err.println("Key:"+key + "Value:"+values);
        for (IntWritable val : values) {
            sum += val.get();
            count++;
        }

        double average = sum/count;
        DecimalFormat df = new DecimalFormat("#.##");
        //df.format(average);
        result.set(Double.parseDouble(df.format(average)));
        context.write(key, result);

    }

}
