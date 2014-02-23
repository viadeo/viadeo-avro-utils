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


    Log log = LogFactory.getLog(ReadMapper.class);


    @Override
    protected void setup(Context context) {
        amos = new AvroMultipleOutputs(context);
    }

    @Override
    protected void reduce(AvroKey<GenericData.Record> record, Iterable<Text> counts, Context context) throws IOException, InterruptedException {
        String file = "";

        int count = 0;

        //context.write(record, NullWritable.get());

        for (Text filename : counts) {
            count = count + 1;
            file = filename.toString();
        }



        log.info("-------" + record + file + count);

        if(count == 1) {
            amos.write("only" + file , record, NullWritable.get());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException,InterruptedException
    {
        amos.close();
    }

}
