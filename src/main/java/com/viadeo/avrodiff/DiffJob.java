package com.viadeo.avrodiff;

import com.viadeo.SchemaUtils;
import com.viadeo.StringUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;

public class DiffJob extends Configured implements Tool {


    public static class DiffMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, Text> {
        public Text fileName;

        @Override
        public void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
            context.write(key, fileName);
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Log log = LogFactory.getLog(DiffMapper.class);

            String name = ((FileSplit) context.getInputSplit()).getPath().getParent().toString() + "/";
            String diffin = context.getConfiguration().get("viadeo.diff.diffinpath");
            String diffout = context.getConfiguration().get("viadeo.diff.diffoutpath");


            String res;
            if (StringUtils.computeLevenshteinDistance(name, diffin) < StringUtils.computeLevenshteinDistance(name, diffout)) {
                res = "del";
            } else {
                res = "add";
            }

            fileName = new Text(res);
            log.info("------------------------------------------------------" + fileName);

        }
    }

    public static class DiffReducer extends Reducer<AvroKey<GenericData.Record>, Text, AvroKey<GenericData.Record>, NullWritable> {

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

            if (count == 2) {
                amos.write("kernel", record, NullWritable.get(), "type=kernel/part");
            }

            if (count == 1) {
                amos.write(side, record, NullWritable.get(), "type=" + side + "/part");
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            amos.close();
        }

    }


    public Job internalRun(Path origInput, Path destInput, Path outputDir, Configuration conf) throws Exception {

        conf.set("viadeo.diff.diffinpath", origInput.toString());
        conf.set("viadeo.diff.diffoutpath", destInput.toString());
        conf.setBoolean("mapred.output.compress", true);


        Job job = new Job(conf);
        job.setJarByClass(DiffJob.class);
        job.setJobName("diff");


        FileSystem fileSystem = FileSystem.get(job.getConfiguration());
        FileStatus[] inputFiles = fileSystem.globStatus(destInput.suffix("/*.avro"));


        if (inputFiles.length == 0) {
            throw new Exception("At least one input is needed");
        }

        String schemaPath = conf.get("viadeo.avro.schema");
        Schema schema;
        if (schemaPath == null)
            schema = SchemaUtils.getSchema(inputFiles[0]);
        else {
            Schema.Parser parser = new Schema.Parser();
            schema = parser.parse(new File(schemaPath));
        }
        //conf.set("avro.schema", schema);


//	    FileInputFormat.setInputPaths(job, origInput);
//	    FileOutputFormat.setOutputPath(job, destInput);

        FileInputFormat.setInputPaths(job, origInput, destInput);
        job.setInputFormatClass(AvroKeyInputFormat.class);

        job.setMapperClass(DiffMapper.class);
        AvroJob.setInputKeySchema(job, schema);
        AvroJob.setMapOutputKeySchema(job, schema);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(DiffReducer.class);
        AvroJob.setOutputKeySchema(job, schema);

        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);


        // ~ OUTPUT
        FileOutputFormat.setOutputPath(job, outputDir);
        AvroMultipleOutputs.addNamedOutput(job, "kernel", AvroKeyOutputFormat.class, schema);
        AvroMultipleOutputs.addNamedOutput(job, "add", AvroKeyOutputFormat.class, schema);
        AvroMultipleOutputs.addNamedOutput(job, "del", AvroKeyOutputFormat.class, schema);

        AvroMultipleOutputs.setCountersEnabled(job, true);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: Read <origin path> <dest path> <output path>");
            return -1;
        }

        Configuration jobConf = getConf();

        Path origPath = new Path(args[0]);
        Path destPath = new Path(args[1]);
        Path outPath = new Path(args[2]);

        Job job = internalRun(origPath, destPath, outPath, jobConf);
        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new Configuration(), new DiffJob(), args);
        System.exit(res);
    }


}
