package com.viadeo.avrondiff;

import com.viadeo.SchemaUtils;
import com.viadeo.StringUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class DiffNJob extends Configured implements Tool {

    public static final String DIFFPATHS = "viadeo.diff.diffinpaths";


    public static class DiffNMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, IntWritable> {

        public IntWritable indexFile;

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

    public static class DiffNReducer extends Reducer<AvroKey<GenericData.Record>, IntWritable, AvroKey<GenericData.Record>, NullWritable> {

        private int sizeOfBA;

        @Override
        protected void reduce(AvroKey<GenericData.Record> record, Iterable<IntWritable> sides, Context context) throws IOException, InterruptedException {
            GenericData.Record datum = record.datum();
            datum.put(SchemaUtils.DIFFBYTEMASK, SchemaUtils.intsToMask(sides, sizeOfBA));
            context.write(new AvroKey<GenericData.Record>(datum), NullWritable.get());
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            sizeOfBA = context.getConfiguration().get(DiffNJob.DIFFPATHS).split(",").length;
        }
    }


    public Job internalRun(String inputDirs, Path outputDir, Configuration conf) throws Exception {

        conf.set(DIFFPATHS, inputDirs);

        Job job = new Job(conf);
        job.setJarByClass(DiffNJob.class);
        job.setJobName("ndiff");

        Schema schema;

        String[] dirs = inputDirs.split(",");


        schema = SchemaUtils.getSchema(conf, new Path(dirs[0]));


        Schema outSchema = SchemaUtils.addByteMask(schema, dirs);
        System.out.println(outSchema);


        FileInputFormat.setInputPaths(job, inputDirs);

        job.setMapperClass(DiffNMapper.class);
        job.setInputFormatClass(AvroKeyInputFormat.class);
        AvroJob.setInputKeySchema(job, outSchema);

        AvroJob.setMapOutputKeySchema(job, outSchema);
        job.setMapOutputValueClass(IntWritable.class);


        job.setReducerClass(DiffNReducer.class);
        AvroJob.setOutputKeySchema(job, outSchema);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);


        // ~ OUTPUT
        FileOutputFormat.setOutputPath(job, outputDir);


        return job;
    }


    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Read <inputdirsCommaSeparated> <output path>");
            for (String arg : args) {
                System.out.println(arg);
            }

            return -1;
        }

        Configuration jobConf = getConf();

        String inputDirs = args[0];
        Path outPath = new Path(args[1]);

        Job job = internalRun(inputDirs, outPath, jobConf);
        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }

        return 0;
    }
}
