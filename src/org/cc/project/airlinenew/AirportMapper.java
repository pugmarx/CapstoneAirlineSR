package org.cc.project.airlinenew;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public  class AirportMapper extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

//        StringTokenizer itr = new StringTokenizer(value.toString(),",");
//        itr.
//        while (itr.hasMoreTokens()) {
//            word.set(itr.nextToken());
//            context.write(word, one);
//        }

        String valueString = value.toString();
        //String[] singleFlightData = valueString.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
        String[] singleFlightData = valueString.split(",");

        context.write(new Text(singleFlightData[7]), one);
        context.write(new Text(singleFlightData[8]), one);

    }
}