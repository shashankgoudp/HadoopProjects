package com.cloudwick.mapreduce.uniquevisitors;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String s = value.toString();
        String words[] = s.split(",");
        String google = "www.google.com";
        
        for(int i = 0;i<words.length;i++)
        {
        	if(words[i].equals(google))
			{
        		context.write(new Text(words[i]), new Text(words[i-1]));
				
			}
        }
        
//        for (String word : s.split(",")) {
//            if (word.length() > 0) {
//                context.write(new Text(word), new Text(word));
//            }
//        }
        
//        String words[] = s.split(",");
//        
//        for(int i =0;i<3;i++)
//        {
//        	if()
//        	
//        }
    }
}
