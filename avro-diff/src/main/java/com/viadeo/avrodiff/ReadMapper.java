package com.viadeo.avrodiff;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class ReadMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, IntWritable> {

	@Override
	public void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
		context.write(key, new IntWritable(1));
	}
}