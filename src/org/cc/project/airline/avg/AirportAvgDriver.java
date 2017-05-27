package org.cc.project.airline.avg;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AirportAvgDriver {
    public static void main(String[] args) throws Exception {

        System.out.println("******* In the driver ********");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ArrDelayAvg");
        job.setJarByClass(AirportAvgDriver.class);
        job.setMapperClass(AirportAvgMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //job.setCombinerClass(AirportAvgReducer.class);
        //job.setPartitionerClass(KeyFieldBasedPartitioner.class);


        job.setReducerClass(AirportAvgReducer.class);
        job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fileStatus = fs.listStatus(new Path(args[0]));

        for(FileStatus status : fileStatus){
            System.out.println("******* input path is ***"+status.getPath());
            MultipleInputs.addInputPath(job, status.getPath(), TextInputFormat.class, AirportAvgMapper.class);
        }

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}