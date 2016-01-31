package com.cloudwick.mapreduce.join;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class Location extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
{
    //variables to process delivery report
    private String IP_ADDR,LOCATION,fileTag="LOC~";
   
   /* map method that process DeliveryReport.txt and frames the initial key value pairs
    Key(Text)  mobile number
    Value(Text)  An identifier to indicate the source of input(using DR for the delivery report file) + Status Code*/

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
    {
       //taking one line/record at a time and parsing them into key value pairs
        String line = value.toString();
        String splitarray[] = line.split(",");
        IP_ADDR = splitarray[0].trim();
        LOCATION = splitarray[1].trim();
       
        //sending the key value pair out of mapper
        output.collect(new Text(IP_ADDR), new Text(fileTag+LOCATION));
     }
}