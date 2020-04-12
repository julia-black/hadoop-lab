package com.ssu.juliablack.hadoop.videowatermark;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.IOException;

public class VideoMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable longWritable, Text text, Context context) throws IOException, InterruptedException {
        String line = text.toString();
        String[] words = line.split(" ");
        if (words.length < 3) {
            throw new IOException("Неверный формат файла");
        }
        File inputFile = new File(words[0]);
        int part = Integer.parseInt(words[1]);
        File outputFile = new File(words[2]);
        VideoUtil.addWaterMark(inputFile, outputFile, part);
        context.write(new Text(words[0]), new Text(part + " " + words[2]));
    }
}