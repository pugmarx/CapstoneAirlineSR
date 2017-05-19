package org.cc.project.sales;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class SalesCountryMapper extends MapReduceBase implements Mapper <LongWritable, Text, Text, IntWritable> {


    private final static IntWritable one = new IntWritable(1);

//    Transaction date, Product,	Price,	Payment Type,	Name,	    City,   	State,	    Country,	    Account Created,	Last Login,	    Latitude,	Longitude
//     01-02-2009 6:17,	Product1,	1200,	Mastercard,     carolina,	Basildon,	England,	United Kingdom,	01-02-2009 6:00,	01-02-2009 6:08,    51.5,	    -1.1166667
//     01-02-2009 4:53,	Product1,	1200,	Visa,	        Betina,	    Parkville,      MO, 	United States,	01-02-2009 4:42,	01-02-2009 7:49,    39.195,	    -94.68194
//     01-02-2009 3:08,	Product1,	1200,	Mastercard,	  Federica e Andrea,Astoria,    OR, 	United States,	01-01-2009 16:21,	01-03-2009 12:32,	46.18806,	-123.83

    public void map(LongWritable key, Text value, OutputCollector <Text, IntWritable> output, Reporter reporter) throws IOException {

        String valueString = value.toString();
        String[] SingleCountryData = valueString.split(",");

        // For every occurrence of Country (0-based index)
        // UK -> 1
        // US -> 1
        // US -> 1
        output.collect(new Text(SingleCountryData[7]), one);


    }



}