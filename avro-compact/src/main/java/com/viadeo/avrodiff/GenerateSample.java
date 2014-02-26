package com.viadeo.avrodiff;



import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.Pair;

import java.io.File;
import java.io.IOException;


public class GenerateSample {

    public static class TestSchema {

        public static final String KEY = "key";
        public static final String VALUE = "value";

        public static Schema getSchema() {
            Schema schema = Pair.getPairSchema(Schema.create(Type.STRING), Schema.create(Type.INT));
            return schema;
        }

        public static Record record(String k,Integer v) {
            Record record = new Record(getSchema());
            record.put(0,k);
            record.put(1,v);

            return record;
        }

        public static void append(DataFileWriter<GenericRecord> dataFileWriter, Record[] records) throws IOException {
            for(Record r:records) {
                dataFileWriter.append(r);
            }
        }
    }


    public static void main(String args[]) throws IOException {

        Schema schema = TestSchema.getSchema();

        File file = new File("test1.avro");

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);


        dataFileWriter.create(schema, file);

        TestSchema.append(dataFileWriter, new Record[]{TestSchema.record("2",2), TestSchema.record("3",3)});

        dataFileWriter.close();


        File file2 = new File("test2.avro");

        DatumWriter<GenericRecord> datumWriter2 = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter2 = new DataFileWriter<GenericRecord>(datumWriter2);


        dataFileWriter2.create(schema, file2);

        TestSchema.append(dataFileWriter2, new Record[]{TestSchema.record("2",3),
                TestSchema.record("2",2), TestSchema.record("1",1)});

        dataFileWriter2.close();

    }

}