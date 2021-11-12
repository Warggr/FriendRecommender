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

public class MatchMakerDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MatchMakerDriver(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s needs two arguments, input and output files\n", getClass().getSimpleName());
            return -1;
        }

        JobConf conf1 = new JobConf(MatchMakerDriver.class);
        conf1.setMapOutputKeyClass(CoupleWritable.class);
        conf1.setMapOutputValueClass(BooleanWritable.class);
        conf1.setOutputKeyClass(CoupleWritable.class);
        conf1.setOutputValueClass(IntWritable.class);

        Job j1 = Job.getInstance(conf1);
        j1.setJarByClass(MatchMakerDriver.class);
        j1.setMapperClass(MatchMakerMapper.class);
        j1.setReducerClass(MatchReducer.class);

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileInputFormat.addInputPath(j1, inputPath);
        FileOutputFormat.setOutputPath(j1, outputPath);
        outputPath.getFileSystem(conf1).delete(outputPath, true); //delete recursively=true

        System.out.println("Job 1 started");
        j1.waitForCompletion(true);
        System.out.println("Job 1 completed");

        return 1;
    }
}
