package com.viadeo;


import com.viadeo.avrocompact.CompactJob;
import com.viadeo.avrodiff.DiffJob;
import com.viadeo.avroextract.ExtractJob;
import com.viadeo.avrondiff.DiffNJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;

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
        } else if (toolName.equals("diffn")) {
            return ToolRunner.run(getConf(), new DiffNJob(), remainingArgs);
        } else if (toolName.equals("extract")) {
            return ToolRunner.run(getConf(), new ExtractJob(), remainingArgs);
        }

        return 0;


    }


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new AvroUtilsJob(), args);
        System.exit(res);
    }

}
