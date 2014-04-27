package com.viadeo.avrodiff;

import java.io.File;

import com.viadeo.AvroUtilTest;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
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

public class DiffTest extends AvroUtilTest  {

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


    public static void assertContainsOnly(String base, String dir, Record record) throws Exception {
        String path = base + dir + "/part-r-00000.avro";

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(new File(path), datumReader);

        //Read only one Record
        GenericRecord user = dataFileReader.next();

        Assert.assertEquals("should only contains ", user, record);

    }


	@Test
	public void test42() throws Exception {

		String outStr = tmpFolder.getRoot().getPath() + "/out-generic2";

        Path diffin 	= new Path( new File(tmpFolder.getRoot(), "a").toURI().toString());
        Path diffout 	= new Path( new File(tmpFolder.getRoot(), "b").toURI().toString());
        Path output 	= new Path(outStr);

        Configuration jobConf = new Configuration();
        DiffJob job = new DiffJob();
        Assert.assertTrue(job.internalRun(diffin, diffout, output, jobConf).waitForCompletion(true));

        String base = outStr + "/";
        assertContainsOnly(base, "type=kernel", TestSchema.record("3", 3));
        assertContainsOnly(base, "type=add", TestSchema.record("4", 4));
        assertContainsOnly(base, "type=del", TestSchema.record("2", 2));
	}
}
