package com.ssu.juliablack.hadoop.pagerank;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRankDriver {
    public static void main(String[] args) throws Exception {

        Path inputDirPath = new Path("src/main/resources/input/pagerank/");
        Path outputDirPath = new Path("src/main/resources/output/pagerank/");
        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", "file:/");
        conf.set("mapreduce.framework.name", "local");

        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(outputDirPath, true);
        fs.setWriteChecksum(false);

        Job job = Job.getInstance(conf, "Page Rank");
        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, inputDirPath);
        FileOutputFormat.setOutputPath(job, outputDirPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}