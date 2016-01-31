package com.cloudwick.mapreduce.join;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ReducerFile extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    
    //Variables to aid the join process
    private String ID,MESSAGE,LOCATION,ID_MESSAGE;
    
    


    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
    {
     while (values.hasNext())
     {
          String currValue = values.next().toString();
          String valueSplitted[] = currValue.split("~");
          /*identifying the record source that corresponds to a cell number
          and parses the values accordingly*/
          if(valueSplitted[0].equals("IPADDR"))
          {
        	  ID=valueSplitted[1].trim();
        	  MESSAGE=valueSplitted[2].trim();
        	  ID_MESSAGE = ID + " " + MESSAGE;
          }
          else if(valueSplitted[0].equals("LOC"))
          {
           //getting the delivery code and using the same to obtain the Message
        	  LOCATION = valueSplitted[1].trim();
          }
     }
     
     //pump final output to file
     if(ID_MESSAGE!=null && LOCATION!=null)
     {
            output.collect(new Text(ID_MESSAGE), new Text(LOCATION));
     }
//     else if(customerName==null)
//            output.collect(new Text("customerName"), new Text(deliveryReport));
//     else if(deliveryReport==null)
//            output.collect(new Text(customerName), new Text("deliveryReport"));
     
 }
   
 
   
}
