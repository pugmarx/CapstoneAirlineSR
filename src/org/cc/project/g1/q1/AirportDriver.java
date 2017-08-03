package org.cc.project.g1.q1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedPartitioner;
import org.cc.project.common.utils.FileUtils;


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

        for (FileStatus status : fileStatus) {
            MultipleInputs.addInputPath(job1, status.getPath(), TextInputFormat.class, AirportMapperStep1.class);
        }
        FileUtils.createJobOutputPath(job1, TEMP_PATH, true);


        job2.setMapperClass(AirportMapperStep2.class);
        job2.setMapOutputKeyClass(AirportKeyStep2.class);
        job2.setMapOutputValueClass(NullWritable.class);
        job2.setReducerClass(AirportReducerStep2.class);
        job2.setOutputKeyClass(AirportKeyStep2.class);
        job2.setOutputValueClass(NullWritable.class);
;

        // Reverse sort
        //job2.setSortComparatorClass(AirportTrafficDescComparator.class);

        // Restrict the key count to 10
        job2.setCombinerClass(AirportCombinerStep2.class);

        FileInputFormat.addInputPath(job2, TEMP_PATH);
        FileUtils.createJobOutputPath(job2, args[1], true);
        if (job1.waitForCompletion(true)) {
            job2.submit();
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }

}
