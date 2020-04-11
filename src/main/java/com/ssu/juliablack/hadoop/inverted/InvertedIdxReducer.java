package com.ssu.juliablack.hadoop.inverted;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

class InvertedIdxReducer extends Reducer<Text, IntWritable, Text, ArrayList<IntWritable>> {
    private static int reduceNumber = 0;

    @Override
    protected void setup(Context context) {
        reduceNumber++;
        System.out.println("Reduce" + reduceNumber + ": Start");
    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        String word = key.toString();
        ArrayList<IntWritable> list = new ArrayList<>();
        for (IntWritable value : values) {
            int number = value.get();
            list.add(new IntWritable(number));
        }
        context.write(new Text(word), list);
    }

    @Override
    protected void cleanup(Context context) {
        System.out.println("Reduce" + reduceNumber + ": End");
    }
}