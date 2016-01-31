package com.cloudwick.mapreduce.join;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class IpAddr extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
{
    //variables to process Consumer Details
    private String ID,MESSAGE,IP_ADDR,ID_MESSAGE;
   
    /* map method that process ConsumerDetails.txt and frames the initial key value pairs
       Key(Text) – mobile number
       Value(Text) – An identifier to indicate the source of input(using ‘CD’ for the customer details file) + Customer Name
     */
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
    {
       //taking one line/record at a time and parsing them into key value pairs
        String line = value.toString();
        String splitarray[] = line.split(",");
        IP_ADDR = splitarray[0].trim();
        ID = splitarray[1].trim();
        MESSAGE = splitarray[2].trim();
        ID_MESSAGE = "IPADDR~"+ID+"~"+MESSAGE;
        
               
       
      //sending the key value pair out of mapper
        output.collect(new Text(IP_ADDR), new Text(ID_MESSAGE));
     }
}
