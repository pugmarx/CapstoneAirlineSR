package org.cc.project.g2.q2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AirportAvgCombinerStep2 extends Reducer<Text, NullWritable, Text, NullWritable> {

    int nCount = 0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        nCount = 0;
    }

    public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        if (nCount < 10) {
            //    for (Text t : values) {
            context.write(key, NullWritable.get());
            nCount++;
            //  }
        }
    }

}
