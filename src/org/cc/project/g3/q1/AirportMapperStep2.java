package org.cc.project.g3.q1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class AirportMapperStep2 extends Mapper<Object, Text, AirportKeyStep2, NullWritable> {


    // Format of 'value' is: ORD   323232
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String valueString = value.toString();
        String[] row = valueString.split("\\s"); // handle one or more spaces in between

        if (row.length > 1) {
            context.write(new AirportKeyStep2(new Text(row[0]), new IntWritable(Integer.valueOf(row[1]))),
                    NullWritable.get());
        }
    }
}