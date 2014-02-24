package com.viadeo.avrodiff;


import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReadReducer2 extends Reducer<AvroKey<GenericData.Record>, Text, AvroKey<GenericData.Record>, NullWritable> {

    private AvroMultipleOutputs amos;

    @Override
    protected void setup(Context context) {
        amos = new AvroMultipleOutputs(context);
    }

    @Override
    protected void reduce(AvroKey<GenericData.Record> record, Iterable<Text> sides, Context context) throws IOException, InterruptedException {
        String side = "";

        int count = 0;

        for (Text filename : sides) {
            count = count + 1;
            side = filename.toString();
        }

        if(count == 2) {
            amos.write("kernel", record, NullWritable.get(), "type=kernel/part");
        }

        if(count == 1) {
            amos.write(side , record, NullWritable.get(), "type=" + side + "/part");
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException,InterruptedException
    {
        amos.close();
    }

}
