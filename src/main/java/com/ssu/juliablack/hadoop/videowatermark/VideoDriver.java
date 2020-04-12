package com.ssu.juliablack.hadoop.videowatermark;

import org.bytedeco.javacv.*;

import java.io.File;
import java.util.concurrent.CountDownLatch;

import static org.bytedeco.ffmpeg.global.avcodec.AV_CODEC_ID_AAC;
import static org.bytedeco.ffmpeg.global.avcodec.AV_CODEC_ID_H264;
import static org.bytedeco.ffmpeg.global.avutil.AV_PIX_FMT_BGR24;
import static org.bytedeco.ffmpeg.global.avutil.AV_PIX_FMT_YUV420P;

public class VideoDriver {

    public static CountDownLatch countDownLatch;

    public static File inputFile = new File("src/main/resources/input/videowatermark/input_video.mp4");

    public static File outputFile = new File("src/main/resources/output/output_video_full.mp4");

    public static File outputFile1 = new File("src/main/resources/output/output_video_1.mp4");
    public static File outputFile2 = new File("src/main/resources/output/output_video_2.mp4");

    public static void main(String[] args) throws Exception {

        countDownLatch = new CountDownLatch(2);

        new Thread(() -> {
            try {
                addWaterMark(1);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        new Thread(() -> {
            try {
                addWaterMark(2);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        combineVideos(getLengthTime());
    }

    public static void addWaterMark(int part) throws FrameGrabber.Exception, FrameRecorder.Exception, FrameFilter.Exception {
        FFmpegFrameGrabber frameGrabber = new FFmpegFrameGrabber(inputFile);
        frameGrabber.start();
        frameGrabber.setTimestamp(0);

        int width = frameGrabber.getImageWidth();
        int height = frameGrabber.getImageHeight();
        int channels = frameGrabber.getAudioChannels();

        FFmpegFrameRecorder frameRecorder;
        if (part == 1) {
            frameRecorder = new FFmpegFrameRecorder(outputFile1, width, height, channels);
        } else {
            frameRecorder = new FFmpegFrameRecorder(outputFile2, width, height, channels);
        }

        int frameRate = (int) frameGrabber.getFrameRate();
        frameRecorder.setFrameRate(frameRate);

        frameRecorder.setSampleRate(frameGrabber.getSampleRate());
        frameRecorder.setAudioBitrate(frameGrabber.getAudioBitrate());
        frameRecorder.setVideoBitrate(frameGrabber.getVideoBitrate());

        frameRecorder.setPixelFormat(AV_PIX_FMT_YUV420P);
        frameRecorder.setVideoCodec(AV_CODEC_ID_H264);
        frameRecorder.setAudioCodec(AV_CODEC_ID_AAC);

        String watermark = "movie=watermark.png[img];[in][img]overlay=W-w-15:15:format=rgb[out]";
        FFmpegFrameFilter frameFilter = new FFmpegFrameFilter(watermark, width, height);
        frameFilter.setPixelFormat(AV_PIX_FMT_BGR24);

        frameFilter.start();
        frameRecorder.start();
        long timestamp = 0;

        long endTime = frameGrabber.getLengthInTime() / 2;
        if (part == 2) {
            timestamp = frameGrabber.getLengthInTime() / 2 + 1;
            endTime = frameGrabber.getLengthInTime();
        }
        frameGrabber.setTimestamp(timestamp);
        while (timestamp <= endTime) {
            Frame frame = frameGrabber.grab();
            if (frame != null) {
                timestamp = frameGrabber.getTimestamp();
                frameFilter.push(frame);
                Frame filteredFrame = frameFilter.pull();
                if (filteredFrame != null) {
                    frameRecorder.record(filteredFrame);
                }
            } else {
                break;
            }
        }
        frameRecorder.stop();
        frameRecorder.release();
        frameFilter.stop();
        frameFilter.release();
        frameGrabber.stop();
        frameGrabber.release();
        countDownLatch.countDown();
    }

    public static void combineVideos(long fullTime) throws FrameGrabber.Exception, FrameRecorder.Exception {
        FFmpegFrameGrabber frameGrabber1 = new FFmpegFrameGrabber(outputFile1);
        FFmpegFrameGrabber frameGrabber2 = new FFmpegFrameGrabber(outputFile2);

        frameGrabber1.start();
        frameGrabber1.setTimestamp(0);

        frameGrabber2.start();
        frameGrabber2.setTimestamp(0);

        int width = frameGrabber1.getImageWidth();
        int height = frameGrabber1.getImageHeight();
        int channels = frameGrabber1.getAudioChannels();

        FFmpegFrameRecorder frameRecorder;
        frameRecorder = new FFmpegFrameRecorder(outputFile, width, height, channels);

        int frameRate = (int) frameGrabber1.getFrameRate();
        frameRecorder.setFrameRate(frameRate);

        frameRecorder.setSampleRate(frameGrabber1.getSampleRate());
        frameRecorder.setAudioBitrate(frameGrabber1.getAudioBitrate());
        frameRecorder.setVideoBitrate(frameGrabber1.getVideoBitrate());

        frameRecorder.setPixelFormat(AV_PIX_FMT_YUV420P);
        frameRecorder.setVideoCodec(AV_CODEC_ID_H264);
        frameRecorder.setAudioCodec(AV_CODEC_ID_AAC);

        frameRecorder.start();
        long timestamp = 0;
        while (timestamp <= fullTime) {
            if (timestamp != frameGrabber1.getLengthInTime()) {
                Frame frame = frameGrabber1.grab();
                if (frame != null) {
                    timestamp = frameGrabber1.getTimestamp();
                    frameRecorder.record(frame);
                } else { //первое видео кончилось
                    timestamp = frameGrabber1.getLengthInTime();
                }
            } else {
                Frame frame = frameGrabber2.grab();
                if (frame != null) {
                    timestamp = frameGrabber1.getLengthInTime() + frameGrabber2.getTimestamp();
                    frameRecorder.record(frame);
                } else {
                    break;
                }
            }
        }
        frameRecorder.stop();
        frameRecorder.release();
        frameGrabber1.stop();
        frameGrabber1.release();
        frameGrabber2.stop();
        frameGrabber2.release();
    }

    private static long getLengthTime() throws FrameGrabber.Exception {
        FFmpegFrameGrabber frameGrabber = new FFmpegFrameGrabber(inputFile);
        frameGrabber.start();
        return frameGrabber.getLengthInTime();
    }
}