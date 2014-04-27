package com.viadeo;


import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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


    public static Schema getSchema(FileStatus fileStatus) throws Exception {
        FsInput input = new FsInput(fileStatus.getPath(), new Configuration());
        DataFileReader<Void> reader = new DataFileReader<Void>(input, new GenericDatumReader<Void>());
        Schema schema = reader.getSchema();
        reader.close();
        input.close();
        return schema;
    }

    public static Schema addByteMask(Schema schema) {

        return addByteMask(schema, new String[]{});

    }

    public static Schema addByteMask(Schema schema, String[] dirs) {

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

            final String fieldSchema = String.format("{\"name\":\"%s\", \"type\":\"bytes\",\"default\":\"0\",\"%s\": \"%s\"}", DIFFBYTEMASK, DIFFDIRSPROPNAME, dirString);

            final JsonFactory jsonFactory = (new ObjectMapper()).getJsonFactory();

            final ObjectNode jsonSchema = (ObjectNode) jsonFactory.createJsonParser(schema.toString()).readValueAsTree();

            final ArrayNode fields = (ArrayNode) jsonSchema.get("fields");

            fields.add(jsonFactory.createJsonParser(fieldSchema).readValueAsTree());

            jsonSchema.put("fields", fields);

            return Schema.parse(jsonSchema.toString());

        } catch (IOException e) {
            throw new RuntimeException("JSONYOLO");

        }
    }


}
