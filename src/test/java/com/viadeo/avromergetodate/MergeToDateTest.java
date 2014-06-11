package com.viadeo.avromergetodate;


import com.viadeo.AvroUtilTest;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class MergeToDateTest extends AvroUtilTest {

    @Before
    public void setUp() throws Exception {


        Schema schema = Schema.parse("{\"type\":\"record\",\n" +
                " \"name\":\"test\",\n" +
                " \"fields\" : [\n" +
                "{\n" +
                "    \"name\" : \"diffmask\",\n" +
                "    \"type\" : \"string\",\n" +
                "    \"default\" : \"\",\n" +
                "    \"diffdirs\" : \"/bi/importdatetime=2014-01-18T01_16_16p01_00,/bi/importdatetime=2014-01-19T01_16_16p01_00,/bi/importdatetime=2014-01-20T01_16_16p01_00\"\n" +
                "    }, {\n" +
                "     \"name\":\"value\",\n" +
                "     \"type\":\"string\"\n" +
                "    }]\n" +
                "    }");


        GenericData.Record record = new GenericData.Record(schema);

        record.put("value", "A");
        record.put("diffmask", "110");
        GenericData.Record record1 = new GenericData.Record(schema);

        record1.put("value", "B");
        record1.put("diffmask", "001");





        GenericData.Record[] records = {record, record1};

        create("in", "input.avro", records);

    }

    @Test
    public void test42() throws Exception {

        String outStr = tmpFolder.getRoot().getPath() + "/out-generic";

        Path diffin = new Path(new File(tmpFolder.getRoot(), "in").toURI().toString());

        MergeToDateJob mergeToDateJob = new MergeToDateJob();

        Configuration jobConf = new Configuration();

        Job job = mergeToDateJob.internalRun(diffin, new Path(outStr), jobConf);

        boolean b = job.waitForCompletion(true);


        Schema resultSchema = Schema.parse("{\n" +
                "  \"type\" : \"record\",\n" +
                "  \"name\" : \"test\",\n" +
                "  \"fields\" : [ {\n" +
                "    \"name\" : \"diffmask\",\n" +
                "    \"type\" : \"string\",\n" +
                "    \"default\" : \"\",\n" +
                "    \"diffdirs\" : \"/bi/importdatetime=2014-01-18T01_16_16p01_00,/bi/importdatetime=2014-01-19T01_16_16p01_00,/bi/importdatetime=2014-01-20T01_16_16p01_00\"\n" +
                "  }, {\n" +
                "    \"name\" : \"value\",\n" +
                "    \"type\" : \"string\"\n" +
                "  }, {\n" +
                "    \"name\" : \"dm_from_datetime\",\n" +
                "    \"type\" : \"long\",\n" +
                "    \"default\" : 0\n" +
                "  }, {\n" +
                "    \"name\" : \"dm_to_datetime\",\n" +
                "    \"type\" : \"long\",\n" +
                "    \"default\" : 0\n" +
                "  } ]\n" +
                "}");


        GenericData.Record record = new GenericData.Record(resultSchema);

        record.put("diffmask", "001");
        record.put("value", "B");
        record.put("dm_from_datetime", 1390090576000L);
        record.put("dm_to_datetime", 1390176976000L);

        assertContains(outStr + "/", record);
    }



}

