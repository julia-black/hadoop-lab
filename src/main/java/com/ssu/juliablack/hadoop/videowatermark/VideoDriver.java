package com.ssu.juliablack.hadoop.videowatermark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bytedeco.javacv.FrameGrabber;
import org.bytedeco.javacv.FrameRecorder;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class VideoDriver {

    public static CountDownLatch countDownLatch;

    public static File inputFile = new File("src/main/resources/input/videowatermark/input_video.mp4");

    public static File outputFile = new File("src/main/resources/output/videowatermark/output_video_full.mp4");

    public static File outputFile1 = new File("src/main/resources/output/videowatermark/output_video_1.mp4");
    public static File outputFile2 = new File("src/main/resources/output/videowatermark/output_video_2.mp4");

    public static void main(String[] args) throws Exception {

        //task 4
        //runWithCountDownLatch();

        //task 6
        runWithHadoop();
    }

    private static void runWithCountDownLatch() throws FrameGrabber.Exception, FrameRecorder.Exception {
        countDownLatch = new CountDownLatch(2);

        new Thread(() -> {
            try {
                VideoUtil.addWaterMark(inputFile, outputFile1, 1);
                countDownLatch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        new Thread(() -> {
            try {
                VideoUtil.addWaterMark(inputFile, outputFile2, 2);
                countDownLatch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        VideoUtil.combineVideos(outputFile1, outputFile2, outputFile, VideoUtil.getLengthTime(inputFile));
    }

    private static void runWithHadoop() throws IOException, ClassNotFoundException, InterruptedException {
        Path inputDirPath = new Path("src/main/resources/input/videohadoop/");
        Path outputDirPath = new Path("src/main/resources/output/videohadoop/");

        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", "file:/");
        conf.set("mapreduce.framework.name", "local");

        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(outputDirPath, true);
        fs.setWriteChecksum(false);

        Job job = Job.getInstance(conf, "Video watermark");
        job.setMapperClass(VideoMapper.class);
        job.setReducerClass(VideoReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inputDirPath);
        FileOutputFormat.setOutputPath(job, outputDirPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}