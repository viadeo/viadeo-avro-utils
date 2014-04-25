package com.viadeo.avroextract;

import com.viadeo.SchemaUtils;
import com.viadeo.StringUtils;
import com.viadeo.avrondiff.DiffNJob;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;


public class ExtractMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, NullWritable> {

    public static int diffindex;

    @Override
    public void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
        byte[] bytemask =  ((java.nio.ByteBuffer) key.datum().get(SchemaUtils.DIFFBYTEMASK)).array();

        if(bytemask[diffindex] == 1) {
            context.write(key, NullWritable.get());
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        diffindex =  Integer.parseInt(context.getConfiguration().get(ExtractJob.DIFFINDEX));
    }




}