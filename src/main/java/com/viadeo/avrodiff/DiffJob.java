package com.viadeo.avrodiff;

import java.io.File;
import java.io.IOException;

import com.viadeo.SchemaUtils;
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
import org.apache.hadoop.util.ToolRunner;

public class DiffJob extends Configured implements Tool {



	public Job internalRun(Path origInput, Path destInput, Path outputDir, Configuration conf) throws Exception {

		conf.set("viadeo.diff.diffinpath", origInput.toString() );
		conf.set("viadeo.diff.diffoutpath", destInput.toString() );
		conf.setBoolean("mapred.output.compress", true);


		Job job = new Job(conf);
	    job.setJarByClass(DiffJob.class);
	    job.setJobName("diff");


		FileSystem fileSystem = FileSystem.get(job.getConfiguration());
		FileStatus[] inputFiles = fileSystem.globStatus(destInput.suffix("/*.avro"));


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
