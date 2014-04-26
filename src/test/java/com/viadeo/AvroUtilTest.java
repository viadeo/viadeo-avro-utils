package com.viadeo;


import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public abstract class AvroUtilTest {


    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();


    private static boolean equals(GenericRecord r1, GenericRecord r2) {
        return 0 == compare(r1,r2);
    }

    private static int compare(GenericRecord r1, GenericRecord r2) {
        return GenericData.get().compare(r1, r2, TestSchema.getSchema());
    }

    public static void assertContains(String base, GenericData.Record record) throws Exception {

        String path = base + "/part-r-00000.avro";

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(new File(path), datumReader);

        while (dataFileReader.hasNext()) {
           GenericRecord user = dataFileReader.next();

           if(equals(user,record)) {
               return;
           }
        }

        Assert.assertTrue("not present", false);

    }

    public void create(String dirname, String filename, GenericData.Record[] records) throws  Exception {


        Schema schema = TestSchema.getSchema();

        File dir = new File(tmpFolder.getRoot(), dirname);

        if(!dir.exists() ){
            dir.mkdirs();
        }

        File file = new File(dir,filename);
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        dataFileWriter.create(schema, file);
        TestSchema.append(dataFileWriter, records);
        dataFileWriter.close();

    }

    public static class TestSchema {

        public static final String KEY = "key";
        public static final String VALUE = "value";

        public static Schema getSchema() {

            ArrayList<Schema.Field> fields = new ArrayList<Schema.Field>();

            fields.add(new Schema.Field("key",Schema.create(Schema.Type.STRING), null, null ));
            fields.add(new Schema.Field("value", Schema.create(Schema.Type.INT), null, null));
            Schema schema = Schema.createRecord("ahoy", null, "plouf", false);
            schema.setFields(fields);

            return schema;
        }

        public static GenericData.Record record(String k,Integer v) {
            GenericData.Record record = new GenericData.Record(getSchema());
            record.put(0,k);
            record.put(1,v);

            return record;
        }

        public static void append(DataFileWriter<GenericRecord> dataFileWriter, GenericData.Record[] records) throws IOException {
            for(GenericData.Record r:records) {
                dataFileWriter.append(r);
            }
        }

        public static GenericData.Record recordWithMask(String k, int v, byte[] mask) {
            GenericData.Record record = new GenericData.Record(SchemaUtils.addByteMask(getSchema()));
            record.put(KEY,k);
            record.put(VALUE,v);
            record.put(SchemaUtils.DIFFBYTEMASK, mask);
            return record;
        }
    }
}
