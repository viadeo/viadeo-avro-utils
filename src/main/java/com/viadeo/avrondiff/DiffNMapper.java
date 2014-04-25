package com.viadeo.avrondiff;


import com.viadeo.StringUtils;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class DiffNMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, IntWritable> {


    public static IntWritable indexFile;

    @Override
    public void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
        context.write(key, indexFile);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        String name = ((FileSplit) context.getInputSplit()).getPath().getParent().toString() + "/";

        String[] diffins = context.getConfiguration().get(DiffNJob.DIFFPATHS).split(",");

        indexFile = new IntWritable(StringUtils.indexOfClosestElement(name, diffins));
    }




}