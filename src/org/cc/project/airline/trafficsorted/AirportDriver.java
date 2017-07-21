package org.cc.project.airline.trafficsorted;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedPartitioner;


public class AirportDriver {
    static final Path TEMP_PATH = new Path("tmp");

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "TopNAirportJob");
        Job job2 = Job.getInstance(conf, "SortAndReduce");

        job1.setJarByClass(AirportDriver.class);
        job1.setMapperClass(AirportMapper.class);
        job1.setCombinerClass(AirportReducer.class);
        job1.setPartitionerClass(KeyFieldBasedPartitioner.class);
        job1.setReducerClass(AirportReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fileStatus = fs.listStatus(new Path(args[0]));

        for(FileStatus status : fileStatus){
            MultipleInputs.addInputPath(job1, status.getPath(), TextInputFormat.class, AirportMapper.class);
        }
        FileOutputFormat.setOutputPath(job1, TEMP_PATH);





        FileInputFormat.addInputPath(job2, TEMP_PATH);
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        System.exit(job1.waitForCompletion(true) ? 0 : 1);

    }

}
