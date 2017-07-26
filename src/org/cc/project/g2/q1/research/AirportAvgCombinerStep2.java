package org.cc.project.g2.q1.research;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AirportAvgCombinerStep2 extends Reducer<Text, NullWritable, Text, NullWritable> {

    int nCount = 0;
    Map<String, Integer> keyMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        nCount = 0;
        keyMap = new HashMap<>();
    }

    public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        String subKey = key.toString().split(",")[0];
        //int val = 1;
        if (keyMap.containsKey(subKey)) {
            nCount = keyMap.get(subKey);
            if (nCount >= 10) {
                return;
            }
        }
        keyMap.put(subKey, nCount++);
        context.write(key, NullWritable.get());
    }

}
