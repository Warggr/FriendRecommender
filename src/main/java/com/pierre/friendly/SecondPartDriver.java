package com.pierre.friendly;

import com.pierre.friendly.Writables.CoupleWritable;
import com.pierre.friendly.Writables.RecommWritable;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SecondPartDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SecondPartDriver(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s needs two arguments, input and output files\n", getClass().getSimpleName());
            return -1;
        }

        JobConf conf2 = new JobConf(SecondPartDriver.class);
        conf2.setMapOutputKeyClass(RecommWritable.class);
        conf2.setMapOutputValueClass(IntWritable.class);
        conf2.setOutputKeyClass(IntWritable.class);
        conf2.setOutputValueClass(Text.class);

        Job job2 = Job.getInstance(conf2);
        job2.setJarByClass(SecondPartDriver.class);
        job2.setMapperClass(FinalMapper.class);
        job2.setReducerClass(FinalReducer.class);

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileInputFormat.addInputPath(job2, inputPath);
        FileOutputFormat.setOutputPath(job2, outputPath);
        outputPath.getFileSystem(conf2).delete(outputPath, true); //delete recursively=true

        System.out.println("Job 2 started");
        job2.waitForCompletion(true);
        System.out.println("Job 2 completed");
        return 1;
    }
}
