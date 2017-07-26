package org.cc.project.g2.q2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class AirportAvgMapper extends Mapper<Object, Text, AirportDestinationKey, IntWritable> {

    // *****************************
    // * Origin, Dest -> DepDelay *
    // * 6, 7 -> 8
    // *****************************
    // NN -> -6 (departed early)
    // NN -> 0
    // NN -> 4
    // ****************************
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String valueString = value.toString();
        String[] singleFlightData = valueString.split(",");

        IntWritable val = new IntWritable(0);

        try {

            int depDelay = singleFlightData[8].length() != 0 ? Double.valueOf(singleFlightData[8]).intValue() : 0;
            val.set(depDelay);
            AirportDestinationKey acKey = new AirportDestinationKey(new Text(singleFlightData[6]), new Text(singleFlightData[7]));
            context.write(acKey, val);

        } catch (NumberFormatException e) {
        }
    }
}