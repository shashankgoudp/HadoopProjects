package com.cloudwick.mapreduce.uniquevisitors;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class SumReducer extends Reducer<Text, Text, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        int wordCount = 0;
//        for (Text value : values) {
//            wordCount += value.get();
//        }
//        context.write(key, new IntWritable(wordCount));
        
        HashSet<Text>  set = new HashSet<Text>();
        for(Text value : values)
        {
        	set.add(value);
        }
        wordCount = set.size();
        context.write(key, new IntWritable(wordCount));
        
    }
}