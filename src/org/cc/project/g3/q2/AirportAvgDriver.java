package org.cc.project.g3.q2;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.cc.project.common.misc.AirlineConstants;
import org.cc.project.common.utils.FileUtils;

import java.text.SimpleDateFormat;

import static org.cc.project.common.misc.AirlineConstants.*;


public class AirportAvgDriver {

    private static void validateInputs(String[] args) throws Exception {
        if (args.length < 6 || StringUtils.isBlank(args[2])
                || StringUtils.isBlank(args[3]) || StringUtils.isBlank(args[4]) || StringUtils.isBlank(args[5])) {
            throw new IllegalArgumentException("Invalid or null input");
        }
        SimpleDateFormat sdf = new SimpleDateFormat(AirlineConstants.FLT_DATE_PROP_FORMAT);
        sdf.parse(args[5]);
    }


    /*
        CMI  ORD  LAX 04/03/2008
    */
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Leg1");
        Job job2 = Job.getInstance(conf, "Leg2");


        validateInputs(args);

        // ** leg 1 props
        job1.getConfiguration().set(ORIGIN_CODE_PROP, args[2]);
        job1.getConfiguration().set(DEST_CODE_PROP, args[3]);
        job1.getConfiguration().set(FLT_LEG1_DATE_PROP, args[5]);
        job1.getConfiguration().set(FLT_LEG_PROP, "leg1");
        //--
        job1.setJarByClass(AirportAvgDriver.class);
        job1.setMapperClass(AirportLegMapper.class);
        job1.setMapOutputKeyClass(AirportDestinationKey.class);
        job1.setMapOutputValueClass(NullWritable.class);
        job1.setReducerClass(AirportLegReducer.class);
        job1.setCombinerClass(AirportLegReducer.class); //FIXME
        job1.setOutputValueClass(NullWritable.class);

        // ** leg 2 props
        job2.getConfiguration().set(ORIGIN_CODE_PROP, args[3]);
        job2.getConfiguration().set(DEST_CODE_PROP, args[4]);
        job2.getConfiguration().set(FLT_LEG1_DATE_PROP, args[5]);
        job2.getConfiguration().set(FLT_LEG_PROP, "leg2");

        //--
        job2.setJarByClass(AirportAvgDriver.class);
        job2.setMapperClass(AirportLegMapper.class);
        job2.setMapOutputKeyClass(AirportDestinationKey.class);
        job2.setMapOutputValueClass(NullWritable.class);
        job2.setReducerClass(AirportLegReducer.class);
        job2.setCombinerClass(AirportLegReducer.class); //FIXME
        job2.setOutputValueClass(NullWritable.class);

        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fileStatus = fs.listStatus(new Path(args[0]));

        for (FileStatus status : fileStatus) {
            MultipleInputs.addInputPath(job1, status.getPath(), TextInputFormat.class, AirportLegMapper.class);
            MultipleInputs.addInputPath(job2, status.getPath(), TextInputFormat.class, AirportLegMapper.class);
        }

        FileUtils.createJobOutputPath(job1, args[1] + "_leg1", true);
        FileUtils.createJobOutputPath(job2, args[1] + "_leg2", true);

        boolean job1RetVal = job1.waitForCompletion(true);
        boolean job2RetVal = job2.waitForCompletion(true);
        System.exit(job1RetVal && job2RetVal ? 0 : 1);

    }

}
