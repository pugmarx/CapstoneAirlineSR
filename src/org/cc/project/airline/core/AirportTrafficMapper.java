package org.cc.project.airline.core;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

//import org.apache.hadoop.mapreduce.Mapper;

//public class AirportTrafficMapper extends MapReduceBase implements Mapper <LongWritable, Text, Text, IntWritable> {

public class AirportTrafficMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    //Origin -> 7, Dest -> 8
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

        String valueString = value.toString();
        //String[] singleFlightData = valueString.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
        String[] singleFlightData = valueString.split(",");

        output.collect(new Text(singleFlightData[7]), one);
        output.collect(new Text(singleFlightData[8]), one);

    }

}