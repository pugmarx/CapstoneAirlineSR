package org.cc.project.g3.q1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.mockito.internal.matchers.Null;

import java.io.IOException;

public class AirportCombinerStep2 extends Reducer<AirportKeyStep2, NullWritable, AirportKeyStep2, NullWritable> {

    int nCount = 0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        nCount = 0;
    }


    public void reduce(AirportKeyStep2 key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        if (nCount < 10) {
            context.write(key, NullWritable.get());
            nCount++;
        }
    }

}