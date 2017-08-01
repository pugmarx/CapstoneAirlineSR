package org.cc.project.common.misc;

/**
 * Created by Hobbes on 26/07/17.
 */
public class AirlineConstants {

    //******** command-line props
    public static final String ORIGIN_CODE_PROP = "originCode";
    public static final String DEST_CODE_PROP = "destCode";
    public static final String FLT_LEG1_DATE_PROP = "flightLeg1Date";
    public static final String FLT_LEG2_DATE_PROP = "flightLeg2Date";
    public static final String FLT_DATE_PROP_FORMAT = "dd/MM/yyyy";
    public static final String FLT_DATE_INPUT_FORMAT = "yyyy-MM-dd";

    //******** indices
    public static final int YEAR_INDEX = 0;
    public static final int FLT_DATE_INDEX = 1;
    public static final int DEP_TIME_INDEX = 2;
    public static final int DAY_OF_WEEK_INDEX = 3;
    public static final int UNIQUE_CARRIER_INDEX = 4;
    public static final int FLIGHT_NUM_INDEX = 5;
    public static final int ORIGIN_INDEX = 6;
    public static final int DEST_INDEX = 7;
    public static final int DEP_DELAY_INDEX = 8;
    public static final int ARR_TIME_INDEX = 9;
    public static final int ARR_DELAY_INDEX = 10;
    public static final int CANCELLED_INDEX = 11;

    //********* misc props
    public static final String TEMP_DIR = "tmp";
    public static final String FLT_LEG_PROP = "flightLeg";

}
