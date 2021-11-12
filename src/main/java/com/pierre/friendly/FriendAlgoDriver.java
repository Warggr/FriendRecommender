package com.pierre.friendly;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

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

        String intermedPath = args[1] + "/first-mapper";
        String[] args1 = {args[0], intermedPath};
        String[] args2 = {intermedPath, args[1]};

        int ret = new MatchMakerDriver().run(args1);
        if(ret != 1) return ret;

        ret = new SecondPartDriver().run(args2);
        if(ret != 1) return ret;
        return 1;
    }
}
