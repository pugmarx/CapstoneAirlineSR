package org.cc.project.g2.q1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public  class AirportAvgMapperStep2 extends Mapper<Object, Text, DoubleWritable, Text>{


    // Format of 'value' is: ORD   323232
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String valueString = value.toString();
        String[] row = valueString.split("\\s"); // handle one or more spaces in between


        // 1.02 -> NXN
        if(row.length>1) {
            context.write(new DoubleWritable(Double.valueOf(row[1])), new Text(row[0]));
        }
    }
}