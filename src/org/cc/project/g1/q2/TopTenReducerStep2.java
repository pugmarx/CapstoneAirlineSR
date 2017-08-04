package org.cc.project.g1.q2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class TopTenReducerStep2 extends Reducer<AirportKeyStep2, NullWritable, AirportKeyStep2, NullWritable> {

    private TreeMap<AirportKeyStep2, NullWritable> avgToAirlineMap = new TreeMap<>();

    public void reduce(AirportKeyStep2 key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {
        for (Text value : values) {

            // There's just one value!!
            // Add this record to our map with the reputation as the key
            avgToAirlineMap.put(key, NullWritable.get());

            // If we have more than ten records, then curtail!
            if (avgToAirlineMap.size() > 10) {
                avgToAirlineMap.remove(avgToAirlineMap.lastKey());
            }
        }

//        for (Map.Entry<AirportKeyStep2, NullWr> entry : avgToAirlineMap.entrySet()) {
//            String val = String.format("%s,%s", entry.getValue(), entry.getKey().toString());
//            context.write(new Text(val), NullWritable.get());
//        }
        for (AirportKeyStep2 aKey : avgToAirlineMap.keySet()) {
            context.write(aKey, NullWritable.get());
        }
    }
}