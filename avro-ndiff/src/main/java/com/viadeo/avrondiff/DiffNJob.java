package com.viadeo.avrondiff;

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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import java.io.File;
import java.io.IOException;

public class DiffNJob extends Configured implements Tool {

    public static final String DIFFPATHS = "viadeo.diff.diffinpaths";

    public static final String DIFFBYTEMASK = "diffbytemask";


    public static Schema addByteMask(Schema schema) {

        return addByteMask(schema, new String[]{});

    }


    public static Schema addByteMask(Schema schema, String[] dirs) {

        try {
            // THANKS TO AVRO V.1.7.1 in CDH 4.1.2

            String dirString = "";
            boolean isFirst = true;
            for (String dir : dirs) {
                if (isFirst) {
                    isFirst = false;
                } else {
                    dirString += ",";
                }
                dirString += dir;

            }

            final String fieldSchema = "{\"name\":\"diffbytemask\", \"type\":\"bytes\", \"default\":\"0\",\"diffdirs\": \"" + dirString + "\"}";

            final JsonFactory jsonFactory = (new ObjectMapper()).getJsonFactory();

            final ObjectNode jsonSchema = (ObjectNode) jsonFactory.createJsonParser(schema.toString()).readValueAsTree();

            final ArrayNode fields = (ArrayNode) jsonSchema.get("fields");

            fields.add(jsonFactory.createJsonParser(fieldSchema).readValueAsTree());

            jsonSchema.put("fields", fields);

            return Schema.parse(jsonSchema.toString());

        } catch (IOException e) {
            throw new RuntimeException("JSONYOLO");

        }
    }

    public static Schema getSchema(FileStatus fileStatus) throws Exception {
        FsInput input = new FsInput(fileStatus.getPath(), new Configuration());
        DataFileReader<Void> reader = new DataFileReader<Void>(input, new GenericDatumReader<Void>());
        Schema schema = reader.getSchema();
        reader.close();
        input.close();
        return schema;
    }

    public Job internalRun(String inputDirs, Path outputDir, Configuration conf) throws Exception {

        conf.set(DIFFPATHS, inputDirs);
        conf.setBoolean("mapred.output.compress", true);


        Job job = new Job(conf);
        job.setJarByClass(DiffNJob.class);
        job.setJobName("diff");


        FileSystem fileSystem = FileSystem.get(job.getConfiguration());

        String[] dirs = inputDirs.split(",");
        FileStatus[] inputFiles = fileSystem.globStatus(new Path(dirs[0]).suffix("/*.avro"));


        if (inputFiles.length == 0) {
            throw new Exception("At least one input is needed");
        }

        String schemaPath = conf.get("viadeo.avro.schema");
        Schema schema;

        if (schemaPath == null)
            schema = getSchema(inputFiles[0]);
        else {
            Schema.Parser parser = new Schema.Parser();
            schema = parser.parse(new File(schemaPath));
        }


        Schema outSchema = addByteMask(schema, dirs);
        System.out.println(outSchema);


        FileInputFormat.setInputPaths(job, inputDirs);
        job.setInputFormatClass(AvroKeyInputFormat.class);

        job.setMapperClass(DiffNMapper.class);
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

    public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new Configuration(), new DiffNJob(), args);
        System.exit(res);
    }


}
