package org.cc.project.g2.q4;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.cc.project.common.utils.FileUtils;

import static org.cc.project.common.misc.AirlineConstants.*;


public class AirportAvgDriver {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "OriginDepDelayAvg");
        //Job job2 = Job.getInstance(conf, "SortAndReduce");

        if (args.length > 3 && StringUtils.isNotBlank(args[2]) && StringUtils.isNotBlank(args[3])) {
            job1.getConfiguration().set(ORIGIN_CODE_PROP, args[2]);
            job1.getConfiguration().set(DEST_CODE_PROP, args[3]);
        }

        job1.setJarByClass(AirportAvgDriver.class);
        job1.setMapperClass(AirportAvgMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setReducerClass(AirportAvgReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(NullWritable.class);

        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fileStatus = fs.listStatus(new Path(args[0]));

        for (FileStatus status : fileStatus) {
            MultipleInputs.addInputPath(job1, status.getPath(), TextInputFormat.class, AirportAvgMapper.class);
        }

        FileUtils.createJobOutputPath(job1, args[1], true);
        System.exit(job1.waitForCompletion(true) ? 0 : 1);

    }
}
