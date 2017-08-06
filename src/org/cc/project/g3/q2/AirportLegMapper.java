package org.cc.project.g3.q2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static org.cc.project.common.misc.AirlineConstants.*;


public class AirportLegMapper extends Mapper<Object, Text, AirportDestinationKey, NullWritable> {

    // ***************************************
    // * Origin, Dest -> ArrDelay + DepDelay *
    // * 6, 7 -> 8                           *
    // ***************************************
    // NN -> -6 (departed early)
    // NN -> 0
    // NN -> 4
    // ****************************
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        Text originCodeExtValue = new Text(context.getConfiguration().get(ORIGIN_CODE_PROP));
        Text transitCodeExtValue = new Text(context.getConfiguration().get(TRANSIT_CODE_PROP));
        Text destCodeExtValue = new Text(context.getConfiguration().get(DEST_CODE_PROP));
        String flightDateExtValue = context.getConfiguration().get(FLT_LEG1_DATE_PROP);
        String leg = context.getConfiguration().get(FLT_LEG_PROP);

        String valueString = value.toString();
        String[] singleFlightData = valueString.split(",");

        try {
            Text origin = new Text(singleFlightData[ORIGIN_INDEX]);
            Text dest = new Text(singleFlightData[DEST_INDEX]);
            String year = singleFlightData[YEAR_INDEX];
            Text fltDate = new Text(singleFlightData[FLT_DATE_INDEX]);
            Text depTime = new Text(singleFlightData[DEP_TIME_INDEX]);
            Text airline = new Text(singleFlightData[UNIQUE_CARRIER_INDEX]);
            Text fltNum = new Text(singleFlightData[FLIGHT_NUM_INDEX]);

            int arrDelay = singleFlightData[ARR_DELAY_INDEX].length() != 0 ?
                    Double.valueOf(singleFlightData[ARR_DELAY_INDEX]).intValue() : 0;


            Validator validator = new Validator(flightDateExtValue);

            if (leg.equals("leg1")) {
                // Discontinue if irrelevant
                if (!origin.equals(originCodeExtValue) || !dest.equals(transitCodeExtValue)) {
                    return;
                }
                if (!validator.isValidLeg1Date(fltDate.toString()) || !validator.isValidLeg1Time(depTime.toString())) {
                    return;
                }
            } else {
                // Discontinue if irrelevant
                if (!origin.equals(transitCodeExtValue) || !dest.equals(destCodeExtValue)) {
                    return;
                }
                if (!validator.isValidLeg2Date(fltDate.toString()) || !validator.isValidLeg2Time(depTime.toString())) {
                    return;
                }
            }

            AirportDestinationKey legKey = new AirportDestinationKey(originCodeExtValue, transitCodeExtValue,
                    destCodeExtValue, new IntWritable(arrDelay), airline, fltNum, depTime, fltDate, new Text(leg),
                    new Text(validator.getFormattedLeg1Date()));
            context.write(legKey, NullWritable.get());

        } catch (NumberFormatException e) {
        }
    }
}