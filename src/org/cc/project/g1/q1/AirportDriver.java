package org.cc.project.g1.q1;

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
        job1.setMapperClass(AirportMapperStep1.class);
        job1.setCombinerClass(AirportReducerStep1.class);
        job1.setPartitionerClass(KeyFieldBasedPartitioner.class);
        job1.setReducerClass(AirportReducerStep1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fileStatus = fs.listStatus(new Path(args[0]));

        for(FileStatus status : fileStatus){
            MultipleInputs.addInputPath(job1, status.getPath(), TextInputFormat.class, AirportMapperStep1.class);
        }
        if(fs.exists(TEMP_PATH)){
            fs.delete(TEMP_PATH, true);
        }
        FileOutputFormat.setOutputPath(job1, TEMP_PATH);

        //job1.waitForCompletion(true);

        job2.setMapperClass(AirportMapperStep2.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setReducerClass(AirportReducerStep2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        // Reverse sort
        job2.setSortComparatorClass(AirportTrafficDescComparator.class);

        // Restrict the key count to 10
        job2.setCombinerClass(AirportCombinerStep2.class);

        FileInputFormat.addInputPath(job2, TEMP_PATH);
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        if (job1.waitForCompletion(true)) {
            job2.submit();
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
        //System.exit(job2.waitForCompletion(true) ? 0 : 1);

    }

}
