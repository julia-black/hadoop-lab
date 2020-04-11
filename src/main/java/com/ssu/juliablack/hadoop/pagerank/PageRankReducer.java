package com.ssu.juliablack.hadoop.pagerank;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PageRankReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private static int reduceNumber = 0;

    @Override
    protected void setup(Context context) {
        reduceNumber++;
        System.out.println("Reduce" + reduceNumber + ": Start");
    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int vertex = Integer.parseInt(key.toString());
        int rank = 0;
        for (IntWritable ignored : values) {
            rank++;
        }
        context.write(new Text(vertex + ""), new IntWritable(rank));
    }

    @Override
    protected void cleanup(Context context) {
        System.out.println("Reduce" + reduceNumber + ": End");
    }
}