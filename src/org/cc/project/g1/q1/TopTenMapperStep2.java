package org.cc.project.g1.q1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.TreeMap;

public class TopTenMapperStep2 extends Mapper<Object, Text, AirportKeyStep2, NullWritable> {

    // Stores a map of user reputation to the record
    private TreeMap<AirportKeyStep2, NullWritable> avgToAirlineMap = new TreeMap<>();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String valueString = value.toString();
        String[] row = valueString.split("\\s"); // handle one or more spaces in between

        // Add this record to our map with the reputation as the key
        AirportKeyStep2 aKey = new AirportKeyStep2(new Text(row[0]), new IntWritable(Integer.parseInt(row[1])));
        avgToAirlineMap.put(aKey, NullWritable.get());

        // If we have more than ten records
        if (avgToAirlineMap.size() > 10) {
            avgToAirlineMap.remove(avgToAirlineMap.lastKey());
        }
    }

    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        for (AirportKeyStep2 key : avgToAirlineMap.keySet()) {
            context.write(key, NullWritable.get());
        }
    }
}
