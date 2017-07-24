package org.cc.project.g1.q2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class AirportAvgMapper extends Mapper<Object, Text, Text, IntWritable> {

    //private final static IntWritable val = new IntWritable(0);
    //private Text word = new Text();

    // *****************************
    // * UniqueCarrier -> ArrDelay *
    // *****************************
    // NN -> -6 (reached early)
    // NN -> 0
    // NN -> 4
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String valueString = value.toString();
        String[] singleFlightData = valueString.split(",");

        IntWritable val = new IntWritable(0);

        try {
            int arrDel = singleFlightData[10].length() != 0 ? Double.valueOf(singleFlightData[10]).intValue() : 0;
            val.set(arrDel);
            context.write(new Text(singleFlightData[4]), val);
        } catch (NumberFormatException e) {
            //e.printStackTrace();
            //ignore
            //return;
        }
    }
}