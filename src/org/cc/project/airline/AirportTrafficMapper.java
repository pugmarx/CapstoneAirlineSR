package org.cc.project.airline;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

//import org.apache.hadoop.mapreduce.Mapper;

//public class AirportTrafficMapper extends MapReduceBase implements Mapper <LongWritable, Text, Text, IntWritable> {

public class AirportTrafficMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    //Origin -> 11, Dest -> 17
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

        String valueString = value.toString();
        //String[] singleFlightData = valueString.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
        String[] singleFlightData = valueString.split(",");

        // FIXME *** Ignoring cancelled flights for now
        // TODO *** Anything else to ignore??

        //String obCode = singleFlightData[11] != null ? singleFlightData[11].substring(1,
        //        singleFlightData[11].length() - 1) : "";
        //String ibCode = new StringBuffer(singleFlightData[17] != null ? singleFlightData[17].substring(1,
        //         singleFlightData[17].length() - 1) : "";

        // For every Origin
        //if (!"Origin".equals(obCode)) {
        //    output.collect(new Text(singleFlightData[11].replaceAll("\"","")), one);
        output.collect(new Text(singleFlightData[11]), one);
        //}
        //if (!"Origin".equals(obCode)) {
        //    output.collect(new Text("OB_" + obCode), one);
       // }

        // For every Dest
       // if (!"Dest".equals(ibCode)) {
       //     output.collect(new Text("IN_" + ibCode), one);
       // }
         //For every Dest
         //if (!"Dest".equals(ibCode)) {
        //output.collect(new Text(singleFlightData[17]), one);
        output.collect(new Text(singleFlightData[18]), one);
           //  output.collect(new Text(singleFlightData[17].replaceAll("\"","")), one);
         //}

    }

    //TODO Optimize??
//    private String stripQuotes(StringBuffer s) {
//        if (s == null || s.length() == 0) {
//            return "";
//        }
//        if (s.charAt(0) == '"') {
//            s.substring(1);
//        }
//        if (s.charAt(s.length() - 1) == '"') {
//            s.substring(0, s.length() - 1);
//        }
//        return s.toString();
//    }

//    // line -> the quick brown fox jumped over the lazy dog
//    public void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
//
//        String csvRecord = value.toString();
//        String[] csvRecordArr = csvRecord.split(",");
//
//        context.write("OB_"+new Text(csvRecordArr[11]), new IntWritable(1));
//        context.write("IB_"+new Text(csvRecordArr[17]), new IntWritable(1));
//
//    }

}