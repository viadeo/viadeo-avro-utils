package com.viadeo.avrodiff;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class DiffJob extends Configured implements Tool {

	public static Schema getSchema(FileStatus fileStatus ) throws Exception {
		FsInput input = new FsInput(fileStatus.getPath(), new Configuration());
		DataFileReader<Void> reader = new DataFileReader<Void>(input, new GenericDatumReader<Void>());
		Schema schema = reader.getSchema();
		reader.close();
		input.close();
		return schema;
	}

	public Job internalRun(Path origInput, Path destInput, Path outputDir, Configuration conf) throws Exception {


		Job job = new Job(conf);
	    job.setJarByClass(DiffJob.class);
	    job.setJobName("read");


		FileSystem fileSystem = FileSystem.get(job.getConfiguration());
		FileStatus[] inputFiles = fileSystem.globStatus(destInput.suffix("/*.avro"));



		if(inputFiles.length == 0){
			throw new Exception("At least one input is needed");
		}

		Schema schema = getSchema(inputFiles[0]);
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
		AvroMultipleOutputs.addNamedOutput(job,"add"   , AvroKeyOutputFormat.class, schema);
		AvroMultipleOutputs.addNamedOutput(job,"del"   , AvroKeyOutputFormat.class, schema);

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

	    internalRun(origPath, destPath, outPath, jobConf);


		return 0;
	}

}
