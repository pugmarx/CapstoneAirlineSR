package org.cc.project.g3.q1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AirportReducerStep2 extends Reducer<AirportKeyStep2, NullWritable, AirportKeyStep2, NullWritable> {

    int nCount = 0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        nCount = 0;
    }


    // key: 44  value (could be): ATL ORD SFO
    // key: 55454 value: APA
    public void reduce(AirportKeyStep2 key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        //for (NullWritable t : values) {
        key.setRank(new IntWritable(++nCount));
        context.write(key, NullWritable.get());
        //}
    }

}
