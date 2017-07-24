package org.cc.project.g1.q2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AirportAvgReducerStep2 extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

    // key: 1.04  value (could be): AT OR FO
    // key: 4.4 value: PA
    public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text t: values){
            context.write(t, key);
        }
    }

}
