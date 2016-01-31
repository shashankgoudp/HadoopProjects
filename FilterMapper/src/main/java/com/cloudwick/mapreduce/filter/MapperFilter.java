package com.cloudwick.mapreduce.filter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class MapperFilter extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String s = value.toString();
        String words[] = s.split(",");
        
        	if(words[0].equals("Sunnyvale"))
        	{
        		context.write(new Text(words[0]), new Text(words[1]));
        	}
        
    }
}