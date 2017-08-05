package org.cc.project.g2.q4;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import static org.cc.project.common.misc.AirlineConstants.*;

import java.io.IOException;


public class AirportAvgMapper extends Mapper<Object, Text, Text, IntWritable> {

    // *****************************
    // * Origin, Dest -> ArrDelay *
    // * 6, 7 -> 8
    // *****************************
    // NN -> -6 (departed early)
    // NN -> 0
    // NN -> 4
    // ****************************
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String originCodeExtValue = context.getConfiguration().get(ORIGIN_CODE_PROP);
        String destCodeExtValue = context.getConfiguration().get(DEST_CODE_PROP);

        String valueString = value.toString();
        String[] singleFlightData = valueString.split(",");
        IntWritable val = new IntWritable(0);

        try {
            String origin = singleFlightData[ORIGIN_INDEX];
            String dest = singleFlightData[DEST_INDEX];

            // Discontinue if irrelevant
            if(StringUtils.isNotBlank(originCodeExtValue) && StringUtils.isNotBlank(destCodeExtValue)){
                if(!origin.equals(originCodeExtValue) || !dest.equals(destCodeExtValue)){
                    return;
                }
            }

            int arrDelay = singleFlightData[ARR_DELAY_INDEX].length() != 0 ?
                    Double.valueOf(singleFlightData[ARR_DELAY_INDEX]).intValue() : 0;
            val.set(arrDelay);
            //AirportDestinationKey acKey = new AirportDestinationKey(new Text(origin),
            //        new Text(dest));
            Text acKey = new Text(String.format("%s,%s", origin, dest));
            context.write(acKey, val);

        } catch (NumberFormatException e) {
        }
    }
}