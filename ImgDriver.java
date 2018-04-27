import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.mapred.TextInputFormat;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;

import org.apache.hadoop.util.ToolRunner;

public class ImgDriver extends Configured implements Tool

{
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        Job job = new Job(conf, "Eliminate redundant files");

        job.setJarByClass(ImgDriver.class);

        job.setMapperClass(ImgDuplicatesMapper.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setReducerClass(ImgDupReducer.class);
        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;


    }
    public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new Configuration(), new ImageDriver(), args);

        System.exit(res);
    }
}