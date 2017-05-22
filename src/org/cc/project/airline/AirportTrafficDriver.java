package org.cc.project.airline;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;

import java.io.IOException;

public class AirportTrafficDriver {

    public static void main(String[] args) throws IOException {

        JobClient my_client = new JobClient();
        // Create a configuration object for the job
        JobConf job_conf = new JobConf(org.cc.project.airline.AirportTrafficDriver.class);
        FileSystem fs = FileSystem.get(job_conf);

        // Set a name of the Job
        job_conf.setJobName("FlightFrequencyPerAirport");

        // Specify data type of output key and value
        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(IntWritable.class);

        // Specify names of Mapper and Reducer Class
        job_conf.setMapperClass(org.cc.project.airline.AirportTrafficMapper.class);
        job_conf.setReducerClass(org.cc.project.airline.AirportTrafficReducer.class);

        // Specify formats of the data type of Input and output
        job_conf.setInputFormat(TextInputFormat.class);
        job_conf.setOutputFormat(TextOutputFormat.class);

        // Set input and output directories using command line arguments,
        //arg[0] = name of input directory on HDFS, and arg[1] =  name of output directory to be created to store the output file.

        //FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000/"), conf);
        int nMap = 0;
        FileStatus[] fileStatus = fs.listStatus(new Path(args[0]));

        for(FileStatus status : fileStatus){
            //System.out.println("::::::Path is::::::"+status.getPath());
            //FileInputFormat.setInputPaths(job_conf, status.getPath());
            MultipleInputs.addInputPath(job_conf, status.getPath(), TextInputFormat.class,
                    org.cc.project.airline.AirportTrafficMapper.class);
            nMap++;
        }

        job_conf.setNumMapTasks(nMap);

        FileOutputFormat.setOutputPath(job_conf, new Path(args[1]));
        my_client.setConf(job_conf);
        try {
            // Run the job
            JobClient.runJob(job_conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}