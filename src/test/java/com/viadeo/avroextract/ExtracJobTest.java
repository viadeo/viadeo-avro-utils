package com.viadeo.avroextract;

import com.viadeo.AvroUtilTest;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static com.viadeo.AvroUtilTest.TestSchema.record;
import static com.viadeo.AvroUtilTest.TestSchema.recordWithMask;


public class ExtracJobTest extends AvroUtilTest {


    @Before
    public void setUp() throws Exception {
        GenericData.Record[] records = {recordWithMask("3", 3, bmask(1, 1), new String[]{"a", "b"}),
                recordWithMask("4", 4, bmask(0, 1)),
                recordWithMask("2", 2, bmask(1, 0))};

        create("in", "input.avro", records);


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


    @Test
    public void test42() throws Exception {

        String outStr = tmpFolder.getRoot().getPath() + "/out-generic2";

        Path in = new Path(new File(tmpFolder.getRoot(), "in").toURI().toString());

        Path out = new Path(outStr);

        Configuration jobConf = new Configuration();
        ExtractJob job = new ExtractJob();
        Assert.assertTrue(job.internalRun(in, "a", out, jobConf).waitForCompletion(true));

        String base = outStr + "/";


        assertContains(base, record("3", 3));
        assertContains(base, record("2", 2));
        assertNotContains(base, record("4", 4));
    }
}
