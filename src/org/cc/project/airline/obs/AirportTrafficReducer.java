package org.cc.project.airline.obs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class AirportTrafficReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text t_key, Iterator<IntWritable> values, OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException {

        Text key = t_key;

        // Skip header keys
        if(key.toString().contains("Origin") ||  key.toString().contains("Dest")){
            return;
        }

        int inOutFltCount = 0;

        // Add 'ones' for each occurrence
        // ATL -> 1 1 1 1 1 1 1 1 1 1 1 1
        // EWR -> 1 1 1 1 1
        while (values.hasNext()) {

            // replace type of value with the actual type of our value
            IntWritable value = values.next();
            inOutFltCount += value.get();

        }

        // US -> 5
        // UK -> 3
        output.collect(key, new IntWritable(inOutFltCount));

    }

    @Override
    public void close(){

    }

//    public void reduce(Text key, Iterable<IntWritable> values, org.apache.hadoop.mapreduce.Reducer.Context context)
//            throws IOException, InterruptedException {
//        int sum = 0;
//
//        for (IntWritable val : values) {
//            sum += val.get();
//        }
//
//        context.write(key, new IntWritable(sum));
//    }


}
