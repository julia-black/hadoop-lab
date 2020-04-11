package com.ssu.juliablack.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    private static int mapNumber = 0;

    @Override
    protected void setup(Context context) {
        mapNumber++;
        System.out.println("Map" + mapNumber + ": Start");
    }

    @Override
    public void map(LongWritable longWritable, Text text, Context context) throws IOException, InterruptedException {
        String line = text.toString();
        line = line.toLowerCase();
        String[] words = line.split(" ");
        for (String word : words) {
            if (word.length() > 0) {
                context.write(new Text(word), ONE);
            }
        }
    }

    @Override
    protected void cleanup(Context context) {
        System.out.println("Map" + mapNumber + ": End");
    }
}
