package com.viadeo.avrondiff;

import java.io.File;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.viadeo.avrondiff.GenerateSample.TestSchema;

public class DiffNTest {

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

        create("b", "input.avro", records2);
    }



    public static void assertAvroEquals(String message, GenericRecord r1, GenericRecord r2) {
        Assert.assertEquals(message, 0, GenericData.get().compare(r1, r2, GenerateSample.TestSchema.getSchema()));

    }

    public static void assertContainsOnly(String base, String mask, Record record) throws Exception {

        String path = base + "/part-r-00000.avro";

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(new File(path), datumReader);




        while (dataFileReader.hasNext()) {
        GenericRecord user = dataFileReader.next();

           ByteBuffer bmask  = (ByteBuffer) user.get(DiffNJob.DIFFBYTEMASK);
           String smask = "";
           for(byte b:bmask.array()) {
               smask = smask + b;
           }

           if(smask.equals(mask)) {
                   record.put(DiffNJob.DIFFBYTEMASK, user.get(DiffNJob.DIFFBYTEMASK));
                  assertAvroEquals("should only contains ", record, user);
                   return;
           }

        }

        Assert.assertTrue("not present", false);
        //Read only one Record



    }


	@Test
	public void test42() throws Exception {

		String outStr = tmpFolder.getRoot().getPath() + "/out-generic2";

        Path diffin 	= new Path( new File(tmpFolder.getRoot(), "a").toURI().toString());
        Path diffout 	= new Path( new File(tmpFolder.getRoot(), "b").toURI().toString());
        Path output 	= new Path(outStr);

        Configuration jobConf = new Configuration();
        DiffNJob job = new DiffNJob();
        Assert.assertTrue(job.internalRun( diffin.toString() + ","  +  diffout.toString(), output, jobConf).waitForCompletion(true));

        String base = outStr + "/";



        assertContainsOnly(base, "11", TestSchema.recordWithMask("3", 3));
        assertContainsOnly(base, "01", TestSchema.recordWithMask("4", 4));
        assertContainsOnly(base, "10", TestSchema.recordWithMask("2", 2));
	}
}
