package org.cc.project.g2.q1;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class AirportAvgMapperStep2 extends Mapper<Object, Text, Text, NullWritable> {


    // Format of 'value' is: ORD,NW     2.95
    //                       ORD,9E     1.01
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String valueString = value.toString();
        String[] row = valueString.split("\\s"); // handle one or more spaces in between

        // [ORD,NW][2.95]
        if (row.length > 1) {
            context.write(new Text(row[0] + "," + row[1]), NullWritable.get());
        }
    }
}