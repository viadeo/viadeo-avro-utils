package com.viadeo.avrodiff;

import java.io.File;

import com.viadeo.AvroUtilTest;
import com.viadeo.avrocompact.CompactJob;
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


public class CompactTest  extends AvroUtilTest {

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
        Record[] records = {TestSchema.record("2", 2)};

        create("a", "input.avro", records);

    }


    public static void assertContainsOnly(String base, Record record) throws Exception {
        String path = base + "/part-r-00000.avro";

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
        Path output 	= new Path(outStr);

        Configuration jobConf = new Configuration();
        CompactJob job = new CompactJob();
        Assert.assertTrue(job.internalRun(diffin, output, jobConf).waitForCompletion(true));

        String base = outStr + "/";
        assertContainsOnly(base , TestSchema.record("2", 2));
	}
}
