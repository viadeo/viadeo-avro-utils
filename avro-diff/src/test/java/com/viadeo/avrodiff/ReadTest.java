package com.viadeo.avrodiff;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.viadeo.avrodiff.GenerateSample.TestSchema;

public class ReadTest {



	//@Rule
	//public TemporaryFolder tmpFolderInput = new TemporaryFolder();

	@Rule
	public TemporaryFolder tmpFolder = new TemporaryFolder();

    public void create(String dirname, String filename, Record[] records) throws  Exception {


        Schema schema = TestSchema.getSchema();

        File dir = new File(tmpFolder.getRoot(), dirname);

        if(!dir.exists() ){
            dir.mkdirs();
        }

        File file = new File(dir,filename);
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        dataFileWriter.create(schema, file);
        TestSchema.append(dataFileWriter,records);
        dataFileWriter.close();

    }


    @Before
    public void setUp() throws Exception {
        Record[] records = {TestSchema.record("2", 2), TestSchema.record("3", 3)};
        Record[] records2 = {TestSchema.record("4", 4), TestSchema.record("3", 3)};

        create("a", "input.avro", records);

        create("b", "input2.avro", records2);
    }


	@Ignore
    @Test
	public void testRWGenericAvro() throws Exception {


        Configuration jobConf = new Configuration();
		Job job = new Job(jobConf);



		// ~ INPUT
		FileInputFormat.setInputPaths(job, new File(tmpFolder.getRoot(), "a/").getAbsolutePath());
		job.setInputFormatClass(AvroKeyInputFormat.class);
		AvroJob.setInputKeySchema(job, GenerateSample.TestSchema.getSchema());



		job.setMapperClass(ReadMapper.class);
		AvroJob.setMapOutputKeySchema(job, GenerateSample.TestSchema.getSchema());
		job.setMapOutputValueClass(IntWritable.class);

		// ~ OUTPUT
		Path outputPath = new Path(tmpFolder.getRoot().getPath() + "/out-generic");


		FileOutputFormat.setOutputPath(job, outputPath);
		job.setReducerClass(ReadReducer.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(AvroKeyOutputFormat.class);
		AvroJob.setOutputKeySchema(job, GenerateSample.TestSchema.getSchema());

		Assert.assertTrue(job.waitForCompletion(true));


        //Thread.sleep(10000000000000000L);
	}


    @Test
    public void testMultipleInputAvro() throws Exception {


        Configuration jobConf = new Configuration();



        Job job = new Job(jobConf);


        String diffin =  new File(tmpFolder.getRoot(), "a").getAbsolutePath();
        String diffout =  new File(tmpFolder.getRoot(), "b").getAbsolutePath();


        jobConf.set("viadeo.diff.diffinpath", diffin );
        jobConf.set("viadeo.diff.diffoutpath", diffout );


        // ~ INPUT
        FileInputFormat.setInputPaths(job, String.format("%s,%s", diffin, diffout));

        job.setInputFormatClass(AvroKeyInputFormat.class);
        Schema schema = TestSchema.getSchema();


        job.setMapperClass(ReadMapper2.class);
        AvroJob.setInputKeySchema(job, schema);
        AvroJob.setMapOutputKeySchema(job, schema);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(ReadReducer2.class);
        AvroJob.setOutputKeySchema(job, schema);

        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);


        // ~ OUTPUT
        Path outputPath = new Path(tmpFolder.getRoot().getPath() + "/out-generic2");
        FileOutputFormat.setOutputPath(job, outputPath);
        AvroMultipleOutputs.addNamedOutput(job, "kernel", AvroKeyOutputFormat.class, schema);
        AvroMultipleOutputs.addNamedOutput(job,"add"   , AvroKeyOutputFormat.class, schema);
        AvroMultipleOutputs.addNamedOutput(job,"del"   , AvroKeyOutputFormat.class, schema);



        System.out.println("------" + tmpFolder.getRoot().getPath());

        Assert.assertTrue(job.waitForCompletion(true));


        Thread.sleep(100000000000L);


    }
}
