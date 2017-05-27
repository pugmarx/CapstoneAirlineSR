package org.cc.project.airline.avg;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class AirportAvgMapper extends Mapper<Object, Text, Text, IntWritable> {

    //private final static IntWritable val = new IntWritable(0);
    //private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String valueString = value.toString();
        String[] singleFlightData = valueString.split(",");

        IntWritable val = new IntWritable(0);
        System.err.println("******"+singleFlightData[6]+"------->"+singleFlightData[18]);
        try {
            val.set(Integer.parseInt(singleFlightData[18]));
        } catch (NumberFormatException e) {
            e.printStackTrace();
            //ignore
            //return;
        }

        // NN -> -6 (reached early)
        // NN -> 0
        // NN -> 4
        context.write(new Text(singleFlightData[6]), val);
    }
}