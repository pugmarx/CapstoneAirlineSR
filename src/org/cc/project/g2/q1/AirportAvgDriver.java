package org.cc.project.g2.q1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AirportAvgDriver {

    static final Path TEMP_PATH = new Path("tmp");

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "ArrDelayAvg");
        //Job job2 = Job.getInstance(conf, "SortAndReduce");

        job1.setJarByClass(AirportAvgDriver.class);
        job1.setMapperClass(AirportAvgMapper.class);
        //job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputKeyClass(AirportCarrierKey.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setReducerClass(AirportAvgReducer.class);
        //job1.setOutputKeyClass(Text.class);
        job1.setMapOutputKeyClass(AirportCarrierKey.class);
        job1.setOutputValueClass(DoubleWritable.class);

        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fileStatus = fs.listStatus(new Path(args[0]));

        for(FileStatus status : fileStatus){
            MultipleInputs.addInputPath(job1, status.getPath(), TextInputFormat.class, AirportAvgMapper.class);
        }

        if(fs.exists(TEMP_PATH)){
            fs.delete(TEMP_PATH, true);
        }
        FileOutputFormat.setOutputPath(job1, TEMP_PATH);

        System.exit(job1.waitForCompletion(true) ? 0 : 1);

    /*    job2.setMapperClass(AirportAvgMapperStep2.class);
        job2.setMapOutputKeyClass(DoubleWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setReducerClass(AirportAvgReducerStep2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        job2.setCombinerClass(AirportAvgCombinerStep2.class);

        FileInputFormat.addInputPath(job2, TEMP_PATH);
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        if (job1.waitForCompletion(true)) {
            job2.submit();
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
*/
    }

}
