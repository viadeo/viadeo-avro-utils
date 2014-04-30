package com.viadeo;


import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.NullNode;
import org.codehaus.jackson.node.ObjectNode;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class SchemaUtils {

    public static final String DIFFBYTEMASK = "diffbytemask";

    public static final String DIFFDIRSPROPNAME = "diffdirs";


    public static Schema removeField(Schema schema, String fieldname) {
        Schema outSchema;
        outSchema = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), schema.isError());

        List<Schema.Field> fields = new ArrayList<Schema.Field>();
        for (Schema.Field f : schema.getFields()) {
            if (!f.name().equals(fieldname)) {
                fields.add(new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultValue()));
            }
        }
        outSchema.setFields(fields);
        return outSchema;
    }


    public static Schema getConfSchema(Configuration conf) throws Exception {
        String schemaPath = conf.get("viadeo.avro.schema");
        if (schemaPath == null) {
            return null;
        } else {
            Schema.Parser parser = new Schema.Parser();
            return parser.parse(new File(schemaPath));
        }
    }

    public static Schema getSchema(Configuration conf, Path dir) throws Exception {
        FileSystem fileSystem = FileSystem.get(conf);
        FileStatus[] inputFiles = fileSystem.globStatus(dir.suffix("/*.avro"));

        if (inputFiles.length == 0) {
            throw new Exception("At least one input is needed");
        }

        return getSchema(inputFiles[0]);
    }


    public static Schema getSchema(FileStatus fileStatus) throws Exception {
        FsInput input = new FsInput(fileStatus.getPath(), new Configuration());
        DataFileReader<Void> reader = new DataFileReader<Void>(input, new GenericDatumReader<Void>());
        Schema schema = reader.getSchema();
        reader.close();
        input.close();
        return schema;
    }

    public static String[] getDiffDirs(Schema schema) {
        return schema.getField(SchemaUtils.DIFFBYTEMASK).getProp(SchemaUtils.DIFFDIRSPROPNAME).split(",");
    }

    public static Schema addByteMask(Schema schema) {

        return addByteMask(schema, new String[]{});

    }

    public static Schema addByteMask(Schema schema, String[] dirs) {

        schema = removeField(schema, DIFFBYTEMASK);

        try {
            // THANKS TO AVRO V.1.7.1 in CDH 4.1.2

            String dirString = "";
            boolean isFirst = true;
            for (String dir : dirs) {
                if (isFirst) {
                    isFirst = false;
                } else {
                    dirString += ",";
                }
                dirString += dir;

            }

            final String fieldSchema = String.format("{\"name\":\"%s\", \"type\":\"bytes\",\"order\":\"ignore\",\"default\":\"0\",\"%s\": \"%s\"}", DIFFBYTEMASK, DIFFDIRSPROPNAME, dirString);

            final JsonFactory jsonFactory = (new ObjectMapper()).getJsonFactory();

            final ObjectNode jsonSchema = (ObjectNode) jsonFactory.createJsonParser(schema.toString()).readValueAsTree();

            final ArrayNode fields = (ArrayNode) jsonSchema.get("fields");

            fields.add(jsonFactory.createJsonParser(fieldSchema).readValueAsTree());

            jsonSchema.put("fields", fields);

            return Schema.parse(jsonSchema.toString());

        } catch (IOException e) {
            throw new RuntimeException("JSON parsing error ", e);
        }
    }

    public static byte[] intsToMask(Iterable<IntWritable> sides, int sizeOfBA) {
        byte[] bts = new byte[sizeOfBA];

        for (IntWritable index : sides) {
            bts[index.get()] = 1;
        }
        return bts;
    }

    public static byte[] bytesBitmask(Iterable<BytesWritable> sides, int sizeOfBA) {
        byte[] bts = new byte[sizeOfBA];

        for (BytesWritable bw : sides) {
            byte[] oldBitmask = bw.getBytes();

            for (int i = 0; i < sizeOfBA; i++) {
                bts[i] |= oldBitmask[i];
            }
        }

        return bts;
    }


    public static byte[] bmask(int... b) {
        byte[] res = new byte[b.length];
        for (int i = 0; i < b.length; i++) {
            res[i] = (byte) b[i];
        }
        return res;
    }

    public static byte[] bmask(byte... b) {
        return b;
    }

    public static Schema constructSchema(Schema schema, Map<String, Schema.Field> fieldMap) {
        Schema outSchema;
        outSchema = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), schema.isError());

        SortedMap<String, Schema.Field> sMap = new TreeMap<String, Schema.Field>();
        for (Schema.Field f : fieldMap.values()) {
            sMap.put(String.format("%03d%s", f.pos(), f.name()), f);
        }

        List<Schema.Field> fields = new ArrayList<Schema.Field>();
        for (Schema.Field f : sMap.values()) {

            Schema fieldSchema = f.schema();

            List<Schema> schemaList = new ArrayList<Schema>();

            // THE ORDER MATTER : https://issues.apache.org/jira/browse/AVRO-1118
            schemaList.add(Schema.create(Schema.Type.NULL));

            if (fieldSchema.getType().equals(Schema.Type.UNION)) {
                for (Schema t : fieldSchema.getTypes()) {
                    if (!t.getType().equals(Schema.Type.NULL)) {
                        schemaList.add(t);
                    }
                }
            } else {
                schemaList.add(fieldSchema);
            }

            Schema fschema = Schema.createUnion(schemaList);
            fields.add(new Schema.Field(f.name(), fschema, f.doc(), NullNode.getInstance()));
        }

        outSchema.setFields(fields);
        return outSchema;
    }
}
