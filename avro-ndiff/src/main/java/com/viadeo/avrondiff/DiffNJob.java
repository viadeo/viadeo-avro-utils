package com.viadeo.avrondiff;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
import org.codehaus.jackson.node.BinaryNode;

public class DiffNJob extends Configured implements Tool {

    public static final String  DIFFPATHS  = "viadeo.diff.diffinpaths";

    public static final String DIFFBYTEMASK = "diffbytemask";


    public static Schema addByteMask(Schema schema)  {
        Schema outSchema;


        outSchema = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), schema.isError());
        Schema.Field bf = new Schema.Field(DIFFBYTEMASK, Schema.create(Schema.Type.BYTES), null,  new BinaryNode(new byte[]{0}));

        List<Schema.Field> fields = new ArrayList<Schema.Field>();
        for(Schema.Field f:schema.getFields())      {
            fields.add(new Schema.Field(f.name(),f.schema(),f.doc(),f.defaultValue()));

        }
        fields.add(bf);

        outSchema.setFields(fields);


        return outSchema;
    }

	public static Schema getSchema(FileStatus fileStatus ) throws Exception {
		FsInput input = new FsInput(fileStatus.getPath(), new Configuration());
		DataFileReader<Void> reader = new DataFileReader<Void>(input, new GenericDatumReader<Void>());
		Schema schema = reader.getSchema();
		reader.close();
		input.close();
		return schema;
	}

	public Job internalRun(String inputDirs, Path outputDir, Configuration conf) throws Exception {

		conf.set(DIFFPATHS, inputDirs );
		conf.setBoolean("mapred.output.compress", true);


		Job job = new Job(conf);
	    job.setJarByClass(DiffNJob.class);
	    job.setJobName("diff");



        FileSystem fileSystem = FileSystem.get(job.getConfiguration());

		FileStatus[] inputFiles = fileSystem.globStatus(new Path(inputDirs.split(",")[0]).suffix("/*.avro"));


		if(inputFiles.length == 0){
			throw new Exception("At least one input is needed");
		}

		String schemaPath = conf.get("viadeo.avro.schema");
		Schema schema;

        if(schemaPath == null)
            schema = getSchema(inputFiles[0]);
        else{
            Schema.Parser parser = new Schema.Parser();
            schema = parser.parse(new File(schemaPath));
        }


        Schema outSchema = addByteMask(schema) ;


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
			for(String arg:args) {
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
