package com.viadeo.avroextract;

import com.viadeo.SchemaUtils;
import com.viadeo.StringUtils;
import com.viadeo.avrondiff.DiffNJob;
import com.viadeo.avrondiff.DiffNMapper;
import com.viadeo.avrondiff.DiffNReducer;
import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.File;
import java.io.IOException;

public class ExtractJob extends Configured implements Tool {

    public static final String DIFFINDEX = "com.viadeo.extract.diffindex";


    public Job internalRun(Path inputdir, String origdir, Path outputdir, Configuration conf) throws Exception {


        conf.setBoolean("mapred.output.compress", true);


        Job job = new Job(conf);
        job.setJarByClass(ExtractJob.class);
        job.setJobName("extract");


        FileSystem fileSystem = FileSystem.get(job.getConfiguration());
        FileStatus[] inputFiles = fileSystem.globStatus(inputdir.suffix("/*.avro"));


        if(inputFiles.length == 0){
            throw new Exception("At least one input is needed");
        }

        String schemaPath = conf.get("viadeo.avro.schema");
        Schema schema;
        if(schemaPath == null)
            schema = SchemaUtils.getSchema(inputFiles[0]);
        else{
            Schema.Parser parser = new Schema.Parser();
            schema = parser.parse(new File(schemaPath));
        }


        String prop = schema.getField(SchemaUtils.DIFFBYTEMASK).getProp(SchemaUtils.DIFFDIRSPROPNAME);

        conf.set(DIFFINDEX, "" + StringUtils.indexOfClosestElement(origdir,prop.split(",")));



        Schema outSchema = SchemaUtils.removeField(schema, SchemaUtils.DIFFBYTEMASK);
        System.out.println(outSchema);


        FileInputFormat.setInputPaths(job, inputdir);
        job.setInputFormatClass(AvroKeyInputFormat.class);

        job.setMapperClass(DiffNMapper.class);
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
