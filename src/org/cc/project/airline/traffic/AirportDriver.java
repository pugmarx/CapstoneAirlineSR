package org.cc.project.airline.traffic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedPartitioner;


public class AirportDriver {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TopNAirportJob");
        job.setJarByClass(AirportDriver.class);
        job.setMapperClass(AirportMapper.class);

        job.setCombinerClass(AirportReducer.class);
        //job.setCombinerClass(AirportCombiner.class);
        job.setPartitionerClass(KeyFieldBasedPartitioner.class);


        job.setReducerClass(AirportReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fileStatus = fs.listStatus(new Path(args[0]));

        for(FileStatus status : fileStatus){
            //System.out.println("::::::Path is::::::"+status.getPath());
            MultipleInputs.addInputPath(job, status.getPath(), TextInputFormat.class, AirportMapper.class);
        }
       // FileInputFormat.addInputPath(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
