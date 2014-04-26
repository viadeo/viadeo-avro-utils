package com.viadeo.avrondiff;

import java.io.File;

import com.viadeo.AvroUtilTest;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DiffNTest  extends AvroUtilTest{


    @Before
    public void setUp() throws Exception {
        Record[] records = {TestSchema.record("2", 2), TestSchema.record("3", 3)};
        Record[] records2 = {TestSchema.record("4", 4), TestSchema.record("3", 3)};

        create("a", "input.avro", records);

        create("b", "input.avro", records2);
    }


    public static byte[] bmask(int...b){
       byte[] res = new byte[b.length];
       for(int i=0;i<b.length;i++) {
           res[i] = (byte) b[i];
       }
       return res;
    }
    public static byte[] bmask(byte...b){
        return b;
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


        assertContains(base, TestSchema.recordWithMask("3", 3, bmask(1,1)));
        assertContains(base, TestSchema.recordWithMask("4", 4, bmask(0,1)));
        assertContains(base, TestSchema.recordWithMask("2", 2, bmask(1,0)));
	}
}
