package org.cc.project.g2.q2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.TreeMap;

public class TopTenMapperStep2 extends Mapper<Object, Text, AirportDestinationKey, NullWritable> {

    // Stores a map of user reputation to the record
    private TreeMap<AirportDestinationKey, NullWritable> avgToAirlineMap = new TreeMap<>();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String valueString = value.toString();
        String[] row = valueString.split(","); // handle one or more spaces in between

        // Add this record to our map with the reputation as the key
        AirportDestinationKey aKey = new AirportDestinationKey(new Text(row[0]), new Text(row[1]));
        aKey.setAvgDepDelay(new DoubleWritable(Double.parseDouble(row[2])));
        avgToAirlineMap.put(aKey, NullWritable.get());

        // If we have more than ten records
        if (avgToAirlineMap.size() > 10) {
            avgToAirlineMap.remove(avgToAirlineMap.lastKey());
        }
    }

    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        for (AirportDestinationKey key : avgToAirlineMap.keySet()) {
            context.write(key, NullWritable.get());
        }
    }
}
