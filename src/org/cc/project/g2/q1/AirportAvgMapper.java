package org.cc.project.g2.q1;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static org.cc.project.common.misc.AirlineConstants.*;


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

        String originCodeProp = context.getConfiguration().get(ORIGIN_CODE_PROP);

        String valueString = value.toString();
        String[] singleFlightData = valueString.split(",");
        IntWritable val = new IntWritable(0);

        try {
            String origin = singleFlightData[ORIGIN_INDEX];

            // Discontinue if invalid
            if (StringUtils.isNotBlank(originCodeProp) && !StringUtils.contains(originCodeProp, origin)) {
                return;
            }

            int depDelay = singleFlightData[DEP_DELAY_INDEX].length() != 0 ?
                    Double.valueOf(singleFlightData[DEP_DELAY_INDEX]).intValue() : 0;
            val.set(depDelay);
            AirportCarrierKey acKey = new AirportCarrierKey(new Text(origin),
                    new Text(singleFlightData[UNIQUE_CARRIER_INDEX]));
            context.write(acKey, val);

        } catch (NumberFormatException e) {
        }
    }
}