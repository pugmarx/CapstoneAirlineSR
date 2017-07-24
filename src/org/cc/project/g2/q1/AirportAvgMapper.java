package org.cc.project.g2.q1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class AirportAvgMapper extends Mapper<Object, Text, AirportCarrierKey, IntWritable> {

    // *****************************
    // * Origin, UniqueCarrier -> DepDelay *
    // * 6, 4 -> 8
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

            AirportCarrierKey acKey = new AirportCarrierKey(new Text(singleFlightData[6]), new Text(singleFlightData[4]));

            context.write(acKey, val);
            //context.write(new Text(singleFlightData[4]), val);
        } catch (NumberFormatException e) {
            //e.printStackTrace();
            //ignore
            //return;
        }
    }
}