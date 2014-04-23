package com.viadeo.avrondiff;


import java.io.IOException;

import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class DiffNReducer extends Reducer<AvroKey<GenericData.Record>, IntWritable, AvroKey<GenericData.Record>, NullWritable> {

    Integer sizeOfBA;



    @Override
    protected void reduce(AvroKey<GenericData.Record> record, Iterable<IntWritable> sides, Context context) throws IOException, InterruptedException {
        byte[] bts = new byte[sizeOfBA];



        for (IntWritable index : sides) {
            bts[index.get()] = 1;
        }


        GenericData.Record datum = record.datum();


        datum.put(DiffNJob.DIFFBYTEMASK, bts);

        context.write(new AvroKey<GenericData.Record>(datum),NullWritable.get());


    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        sizeOfBA =  context.getConfiguration().get(DiffNJob.DIFFPATHS).split(",").length;

    }
}
