package com.viadeo.avrodiff;

import java.io.IOException;

import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ReadReducer extends Reducer<AvroKey<GenericData.Record>, IntWritable, AvroKey<GenericData.Record>, NullWritable> {

    @Override
    protected void reduce(AvroKey<GenericData.Record> record, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable count : counts) {
        sum += count.get();
      }

      context.write(record, NullWritable.get());
    }

}
