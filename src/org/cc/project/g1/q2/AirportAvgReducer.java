package org.cc.project.g1.q2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

public class AirportAvgReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {


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
        //result.set(Double.parseDouble(df.format(average)));
        //context.write(key, result);
        String val = String.format("%s,%s", key, Double.parseDouble(df.format(average)));
        context.write(new Text(val), NullWritable.get());
    }

}
