package com.viadeo.avrocompact;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


public class CompactJob extends Configured implements Tool {

    public static Schema getSchema(FileStatus fileStatus) throws Exception {
        FsInput input = new FsInput(fileStatus.getPath(), new Configuration());
        DataFileReader<Void> reader = new DataFileReader<Void>(input, new GenericDatumReader<Void>());
        Schema schema = reader.getSchema();
        reader.close();
        input.close();
        return schema;
    }

    public Job internalRun(Path inputDir, Path outputDir, Configuration conf) throws Exception {


        conf.setBoolean("mapred.output.compress", true);


        Job job = new Job(conf);
        job.setJarByClass(CompactJob.class);
        job.setJobName("compact");


        FileSystem fileSystem = FileSystem.get(job.getConfiguration());
        FileStatus[] inputFiles = fileSystem.globStatus(inputDir.suffix("/*.avro"));


        if (inputFiles.length == 0) {
            throw new Exception("At least one input is needed");
        }

        Schema schema = getSchema(inputFiles[0]);


        FileInputFormat.setInputPaths(job, inputDir);

        job.setMapperClass(Mapper.class);
        // MAPPER INPUT SPEC
        job.setInputFormatClass(AvroKeyInputFormat.class);
        AvroJob.setInputKeySchema(job, schema);

        // MAPPER OUTPUT SPEC + REDUCER INPUT SPEC
        AvroJob.setMapOutputKeySchema(job, schema);
        job.setMapOutputValueClass(NullWritable.class);


        job.setReducerClass(Reducer.class);
        // REDUCER OUTPUT SPEC
        AvroJob.setOutputKeySchema(job, schema);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);

        // ~ OUTPUT
        FileOutputFormat.setOutputPath(job, outputDir);


        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Read <input path> <output path>");
            return -1;
        }

        Configuration jobConf = getConf();

        Path origPath = new Path(args[0]);
        Path outPath = new Path(args[1]);

        Job job = internalRun(origPath, outPath, jobConf);
        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new Configuration(), new CompactJob(), args);
        System.exit(res);
    }


}
