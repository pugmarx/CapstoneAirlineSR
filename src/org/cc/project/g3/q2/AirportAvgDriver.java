package org.cc.project.g3.q2;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.cc.project.common.misc.AirlineConstants;
import org.cc.project.common.utils.FileUtils;

import java.text.SimpleDateFormat;

import static org.cc.project.common.misc.AirlineConstants.DEST_CODE_PROP;
import static org.cc.project.common.misc.AirlineConstants.FLT_DATE_PROP;
import static org.cc.project.common.misc.AirlineConstants.ORIGIN_CODE_PROP;


public class AirportAvgDriver {

    private static void validateInputs(String[] args) throws Exception{
        if (args.length < 5 || StringUtils.isBlank(args[2])
                || StringUtils.isBlank(args[3]) || StringUtils.isBlank(args[4])) {
            throw new IllegalArgumentException("Invalid or null input");
        }

        SimpleDateFormat sdf = new SimpleDateFormat(AirlineConstants.FLT_DATE_PROP_FORMAT);
        sdf.format(args[4]);
    }



    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Leg1");
        Job job2 = Job.getInstance(conf, "Leg2");

        validateInputs(args);

        job1.getConfiguration().set(ORIGIN_CODE_PROP, args[2]);
        job1.getConfiguration().set(DEST_CODE_PROP, args[3]);
        job1.getConfiguration().set(FLT_DATE_PROP, args[4]);

        job1.setJarByClass(AirportAvgDriver.class);
        job1.setMapperClass(AirportAvgMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setReducerClass(AirportAvgReducer.class);
        job1.setOutputValueClass(DoubleWritable.class);

        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fileStatus = fs.listStatus(new Path(args[0]));

        for (FileStatus status : fileStatus) {
            MultipleInputs.addInputPath(job1, status.getPath(), TextInputFormat.class, AirportAvgMapper.class);
        }

        FileUtils.createJobOutputPath(job1, args[1], true);
        System.exit(job1.waitForCompletion(true) ? 0 : 1);

    }

}
