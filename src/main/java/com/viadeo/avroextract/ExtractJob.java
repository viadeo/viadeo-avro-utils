package com.viadeo.avroextract;

import com.viadeo.SchemaUtils;
import com.viadeo.StringUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class ExtractJob extends Configured implements Tool {

    public static final String DIFFINDEX = "com.viadeo.extract.diffindex";


    public static class ExtractMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, NullWritable> {

        private int diffindex;

        @Override
        public void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
            byte[] bytemask = ((java.nio.ByteBuffer) key.datum().get(SchemaUtils.DIFFBYTEMASK)).array();

            if (bytemask[diffindex] == 1) {
                context.write(key, NullWritable.get());
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            diffindex = Integer.parseInt(context.getConfiguration().get(ExtractJob.DIFFINDEX));
        }


    }


    public Job internalRun(Path inputdir, String origdir, Path outputdir, Configuration conf) throws Exception {


        conf.setBoolean("mapred.output.compress", true);


        Job job = new Job(conf);
        job.setJarByClass(ExtractJob.class);
        job.setJobName("extract");

        Schema schema = SchemaUtils.getSchema(conf, inputdir);

        String[] prop = SchemaUtils.getDiffDirs(schema);

        String indexValue = Integer.toString(StringUtils.indexOfClosestElement(origdir, prop));
        job.getConfiguration().set(DIFFINDEX, indexValue);


        Schema outSchema = SchemaUtils.removeField(schema, SchemaUtils.DIFFBYTEMASK);
        System.out.println(outSchema);


        FileInputFormat.setInputPaths(job, inputdir);
        job.setInputFormatClass(AvroKeyInputFormat.class);

        job.setMapperClass(ExtractMapper.class);
        AvroJob.setInputKeySchema(job, schema);
        AvroJob.setMapOutputKeySchema(job, outSchema);


        job.setMapOutputValueClass(NullWritable.class);
        job.setReducerClass(Reducer.class);
        AvroJob.setOutputKeySchema(job, outSchema);

        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);


        // ~ OUTPUT
        FileOutputFormat.setOutputPath(job, outputdir);


        return job;
    }


    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: Read <inputdifffile> <origdir> <output path>");
            for (String arg : args) {
                System.out.println(arg);
            }

            return -1;
        }


        Configuration jobConf = getConf();

        Path inputDir = new Path(args[0]);

        String origDir = args[1];
        Path outPath = new Path(args[2]);

        Job job = internalRun(inputDir, origDir, outPath, jobConf);
        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }

        return 0;

    }
}
