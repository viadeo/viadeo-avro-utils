package com.viadeo.mutationstat;

import com.viadeo.AvroUtilTest;
import com.viadeo.avrodiffstat.MutationStatJob;
import com.viadeo.avromerge.MergeJob;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Map;

import static com.viadeo.AvroUtilTest.TestSchema.*;
import static com.viadeo.SchemaUtils.bmask;


public class MutationTest extends AvroUtilTest {

    @Before
    public void setUp() throws Exception {
        GenericData.Record[] records = {
                recordWithMask("3", 3, bmask(1, 0), new String[]{"a", "b"}),
                recordWithMask("3", 4, bmask(0, 1)),
                recordWithMask("3", 5, bmask(0, 1)),


                recordWithMask("8", 4, bmask(1, 0)),
                recordWithMask("8", 5, bmask(0, 1)),


                recordWithMask("4", 4, bmask(0, 1)),
                recordWithMask("2", 2, bmask(1, 0))
        };

        create("ma", "input.avro", records);

    }


    @Test
    public void test42() throws Exception {

        String outStr = tmpFolder.getRoot().getPath() + "/out-generic";

        Path diffin = new Path(new File(tmpFolder.getRoot(), "ma").toURI().toString());



        Path output = new Path(outStr);

        Configuration jobConf = new Configuration();
        MutationStatJob job = new MutationStatJob();
        Assert.assertTrue(job.groupByKeyRun(diffin, output, "key", jobConf).waitForCompletion(true));



        Path output2 =  new Path(tmpFolder.getRoot().getPath() + "/out-generic2");

        Assert.assertTrue(job.accByResult(output, output2, jobConf).waitForCompletion(true));

        /*
        Thread.sleep(100000000000l);


        String base = outStr + "/";


        assertContains(base, TestSchema.recordWithMask("3", 3, bmask(1, 1, 0, 0)));
        assertContains(base, TestSchema.recordWithMask("4", 4, bmask(0, 1, 1, 0)));
        assertContains(base, TestSchema.recordWithMask("2", 2, bmask(1, 0, 1, 0)));
        assertContains(base, TestSchema.recordWithMask("6", 6, bmask(0, 0, 0, 1)));
        assertContains(base, TestSchema.recordWithMask("5", 5, bmask(0, 1, 1, 1))); */
    }


}
