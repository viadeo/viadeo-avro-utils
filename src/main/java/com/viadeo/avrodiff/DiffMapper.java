package com.viadeo.avrodiff;


import com.viadeo.StringUtils;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class DiffMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, Text> {

    public static Text fileName;

    @Override
    public void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
        context.write(key, fileName);

    }

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {




        Log log = LogFactory.getLog(DiffMapper.class);

        String name = ((FileSplit) context.getInputSplit()).getPath().getParent().toString() + "/";

        log.info("--------------------name: " + name);

                  // jobConf.set("viadeo.diff.diffinpath", diffin );
        //jobConf.set("viadeo.diff.diffoutpath", diffout );

        String diffin = context.getConfiguration().get("viadeo.diff.diffinpath");
        String diffout = context.getConfiguration().get("viadeo.diff.diffoutpath");

        log.info("--------------------------------------------------------------------diffin: " + diffin);
        log.info("--------------------------------------------------------------------diffout: " + diffout);


        String res;
        if(StringUtils.computeLevenshteinDistance(name,diffin) < StringUtils.computeLevenshteinDistance(name,diffout) ) {
            res = "del";
        }               else {
            res = "add";
        }

        fileName = new Text(res);
        log.info("------------------------------------------------------" + fileName);

    }



}