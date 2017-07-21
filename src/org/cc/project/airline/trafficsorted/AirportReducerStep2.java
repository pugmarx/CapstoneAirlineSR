package org.cc.project.airline.trafficsorted;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AirportReducerStep2 extends Reducer<IntWritable, Text, Text, IntWritable> {

//    private IntWritable result = new IntWritable();

//    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//
//        int sum = 0;
//        for (IntWritable val : values) {
//            sum += val.get();
//        }
//        result.set(sum);
//        context.write(key, result);
//    }

    // key: 44  value (could be): ATL ORD SFO
    // key: 55454 value: APA
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text t: values){
            context.write(t, key);
        }
    }

}
