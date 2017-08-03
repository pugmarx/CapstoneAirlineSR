package org.cc.project.g1.q2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.cc.project.common.utils.FileUtils;


public class AirportAvgDriver {

    static final Path TEMP_PATH = new Path("tmp");

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "ArrDelayAvg");
        Job job2 = Job.getInstance(conf, "SortAndReduce");

        job1.setJarByClass(AirportAvgDriver.class);
        job1.setMapperClass(AirportAvgMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setReducerClass(AirportAvgReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);

        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fileStatus = fs.listStatus(new Path(args[0]));

        for (FileStatus status : fileStatus) {
            MultipleInputs.addInputPath(job1, status.getPath(), TextInputFormat.class, AirportAvgMapper.class);
        }

        FileUtils.createJobOutputPath(job1, TEMP_PATH, true);

        job2.setMapperClass(AirportAvgMapperStep2.class);
        //job2.setMapOutputKeyClass(DoubleWritable.class);
        //job2.setMapOutputValueClass(Text.class);
        job2.setMapOutputKeyClass(AirportAvgDriver.class);
        job2.setMapOutputValueClass(NullWritable.class);

        job2.setReducerClass(AirportAvgReducerStep2.class);
        job2.setOutputKeyClass(AirportKeyStep2.class);
        job2.setOutputValueClass(NullWritable.class);
        //job2.setOutputKeyClass(Text.class);
        //job2.setOutputValueClass(DoubleWritable.class);
        job2.setCombinerClass(AirportAvgCombinerStep2.class);

        FileInputFormat.addInputPath(job2, TEMP_PATH);
        //FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        FileUtils.createJobOutputPath(job2, args[1], true);

        if (job1.waitForCompletion(true)) {
            job2.submit();
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }

    }

}
