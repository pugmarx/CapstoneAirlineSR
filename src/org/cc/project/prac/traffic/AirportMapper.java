package org.cc.project.prac.traffic;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public  class AirportMapper extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    //private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String valueString = value.toString();
        String[] singleFlightData = valueString.split(",");

        context.write(new Text(singleFlightData[6]), one);
        context.write(new Text(singleFlightData[7]), one);
    }
}