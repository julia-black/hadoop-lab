package com.ssu.juliablack.hadoop.videowatermark;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.File;
import java.io.IOException;

public class VideoReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String inputFile = key.toString();
        File outputFile1 = null;
        File outputFile2 = null;
        for (Text value : values) {
            String[] words = value.toString().split(" ");
            int part = Integer.parseInt(words[0]);
            String filePath = words[1];
            if (part == 1) {
                outputFile1 = new File(filePath);
            } else {
                outputFile2 = new File(filePath);
            }
        }
        String outputFileFullPath = "src/main/resources/output/videohadoop/output_video_full.mp4";
        File outputFileFull = new File(outputFileFullPath);
        if (outputFile1 == null || outputFile2 == null) {
            throw new IOException("Не удалось получить видео-файлы");
        }
        VideoUtil.combineVideos(outputFile1, outputFile2, outputFileFull, VideoUtil.getLengthTime(new File(inputFile)));

        context.write(new Text(outputFileFullPath), new Text());
    }
}