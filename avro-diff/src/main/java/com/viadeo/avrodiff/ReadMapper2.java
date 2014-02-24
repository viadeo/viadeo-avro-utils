package com.viadeo.avrodiff;


import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class ReadMapper2 extends Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, Text> {

    public static Text fileName;

    @Override
    public void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
        context.write(key, fileName);



    }

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {



        Log log = LogFactory.getLog(ReadMapper2.class);

        String name = ((FileSplit) context.getInputSplit()).getPath().getParent().toString();

        log.info("--------------------name" + name);

                  // jobConf.set("viadeo.diff.diffinpath", diffin );
        //jobConf.set("viadeo.diff.diffoutpath", diffout );

        String diffin = context.getConfiguration().get("viadeo.diff.diffinpath");
        String diffout = context.getConfiguration().get("viadeo.diff.diffoutpath");



        String res;
        // TODO : get from context
        if(name.equals(diffin)) {
            res = "del";
        }               else {
            res = "add";
        }

        fileName = new Text(res);
        log.info("------------------------------------------------------" + fileName);

    }
}