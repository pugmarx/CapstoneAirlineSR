package org.cc.project.airline.avg;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AirportAvgReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

//        // Skip header keys
//        if (key != null && (key.toString().contains("Origin") || key.toString().contains("Dest"))) {
//            return;
//        }


        int sum = 0;
        int count = 1;
        System.err.println("Key:"+key + "Value:"+values);
        for (IntWritable val : values) {
            sum += val.get();
            count++;
        }

        //result.set(sum/count);
        result.set(sum/3);
        context.write(key, result);

    }

}
