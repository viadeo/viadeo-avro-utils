package com.viadeo.avrodiff;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ReadMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, IntWritable> {

    public static Text fileName;

	@Override
	public void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
		context.write(key, new IntWritable(1));
	}

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        String name = ((FileSplit) context.getInputSplit()).getPath().getName();
        fileName = new Text(name);
        System.out.println("------------------------------------------------------" +fileName);

        //context.write(new Text("a"), fileName);
    }
}