package org.cc.project.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by Hobbes on 16/05/17.
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();


    // line -> the quick brown fox jumped over the lazy dog
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


        String line = value.toString();

        StringTokenizer tokenizer = new StringTokenizer(line); // splits it by whitespace

        while (tokenizer.hasMoreTokens()) {

            word.set(tokenizer.nextToken());
            context.write(word, one);

        }


    }


}