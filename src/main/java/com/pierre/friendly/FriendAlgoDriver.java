package com.pierre.friendly;

import org.apache.hadoop.conf.Configuration;
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
import com.pierre.friendly.Writables.CoupleWritable;
import com.pierre.friendly.Writables.RecommWritable;

public class FriendAlgoDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new FriendAlgoDriver(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s needs two arguments, input and output files\n", getClass().getSimpleName());
            return -1;
        }

        JobConf conf1 = new JobConf(FriendAlgoDriver.class);
        conf1.setMapOutputKeyClass(CoupleWritable.class);
        conf1.setMapOutputValueClass(BooleanWritable.class);
        conf1.setOutputKeyClass(CoupleWritable.class);
        conf1.setOutputValueClass(IntWritable.class);

        Job j1 = Job.getInstance(conf1);
        j1.setJarByClass(FriendAlgoDriver.class);
        j1.setMapperClass(MatchMakerMapper.class);
        j1.setReducerClass(MatchReducer.class);

        Path intermedPath = new Path("first-mapper");
        FileInputFormat.addInputPath(j1, new Path(args[0]));
        FileOutputFormat.setOutputPath(j1, intermedPath);
        intermedPath.getFileSystem(conf1).delete(intermedPath, true); //delete recursively=true

        System.out.println("Job 1 started");

        JobConf conf2 = new JobConf();
        conf2.setMapOutputKeyClass(RecommWritable.class);
        conf2.setMapOutputValueClass(IntWritable.class);
        conf2.setOutputKeyClass(IntWritable.class);
        conf2.setOutputValueClass(Text.class);

        Job job2 = Job.getInstance(conf2);
        job2.setJarByClass(FriendAlgoDriver.class);
        job2.setMapperClass(FinalMapper.class);
        job2.setReducerClass(FinalReducer.class);

        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(conf1).delete(outputPath, true); //delete recursively=true
        FileInputFormat.addInputPath(job2, intermedPath);
        FileOutputFormat.setOutputPath(job2, outputPath);

        System.out.println("Waiting for job 1 to complete...");
        j1.waitForCompletion(true);
        System.out.println("Job 1 completed - starting job 2");

        JobClient.runJob(conf2);
        return 1;
    }
}
