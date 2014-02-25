package com.viadeo.avrodiff;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class Read extends Configured implements Tool {

	public static Schema getSchema(FileStatus fileStatus ) throws Exception {
		FsInput input = new FsInput(fileStatus.getPath(), new Configuration());
		DataFileReader<Void> reader = new DataFileReader<Void>(input, new GenericDatumReader<Void>());
		Schema schema = reader.getSchema();
		reader.close();
		input.close();
		return schema;
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: Read <input path> <output path>");
			return -1;
		}

		Configuration jobConf = getConf();
	    Job job = new Job(jobConf);
	    job.setJarByClass(Read.class);
	    job.setJobName("read");




	    Path inputPath = new Path(args[0]);
	    Path outputPath = new Path(args[1]);

		FileSystem fileSystem = FileSystem.get(job.getConfiguration());
		FileStatus[] inputFiles = fileSystem.globStatus(outputPath.suffix("/*.avro"));



		if(inputFiles.length == 0){
			throw new Exception("At least one input is needed");
		}

		String schema = getSchema(inputFiles[0]).toString();
		jobConf.set("avro.schema", schema);


	    FileInputFormat.setInputPaths(job, inputPath);
	    FileOutputFormat.setOutputPath(job, outputPath);


		return 0;
	}

}
