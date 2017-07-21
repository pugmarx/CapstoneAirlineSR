package org.cc.project.airline.trafficsorted;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AirportCombinerStep2 extends Reducer<IntWritable, Text, IntWritable, Text> {

    int nCount = 0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        nCount = 0;
    }


    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if(nCount < 10) {
            for (Text t : values) {
                context.write(key, t);
                nCount++;
            }
        }
    }

}
