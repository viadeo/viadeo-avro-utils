package com.viadeo;


import com.viadeo.avrocompact.CompactJob;
import com.viadeo.avrodiff.DiffJob;
import com.viadeo.avroextract.ExtractJob;
import com.viadeo.avromerge.MergeJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

public class AvroUtilsJob extends Configured implements Tool {


    @Override
    public int run(String[] strings) throws Exception {


        String[] remainingArgs = Arrays.copyOfRange(strings, 1, strings.length);

        String toolName = strings[0];

        System.out.println("starting " + toolName + " Job");

        if (toolName.equals("compact")) {
            return ToolRunner.run(getConf(), new CompactJob(), remainingArgs);
        } else if (toolName.equals("diff")) {
            return ToolRunner.run(getConf(), new DiffJob(), remainingArgs);
        } else if (toolName.equals("extract")) {
            return ToolRunner.run(getConf(), new ExtractJob(), remainingArgs);
        } else if (toolName.equals("merge")) {
            return ToolRunner.run(getConf(), new MergeJob(), remainingArgs);
        } else {
            System.err.println("Wrong tool. Available: diff|extract|merge");
            return -1;
        }
    }

    public static int runAndWatchJobThenMv(final Job job, Path tmp, Path out) throws Exception {
        FileSystem fileSystem = FileSystem.get(job.getConfiguration());

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    job.killJob();
                } catch (Exception e) {
                }
            }
        });

        boolean b = job.waitForCompletion(true);

        if (b) {
            System.out.println("Moving from tmp " + tmp + " to " + out);
            fileSystem.mkdirs(out);
            fileSystem.delete(out,false);
            fileSystem.rename(tmp, out);
        } else {
            throw new IOException("error with job!");
        }

        return 0;
    }


    public static Path tempDirectory() {
        UUID uuid = UUID.randomUUID();

        return new Path("/tmp/uuid=" + uuid.toString());
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setBoolean("mapred.output.compress", true);
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
        conf.setInt("mapred.reduce.tasks", 30);
        conf.set("mapred.child.java.opts", "-Xmx512m");

        int res = ToolRunner.run(conf, new AvroUtilsJob(), args);
        System.exit(res);
    }

}
