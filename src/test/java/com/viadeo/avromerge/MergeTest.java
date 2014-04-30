package com.viadeo.avromerge;

import com.viadeo.AvroUtilTest;
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

public class MergeTest extends AvroUtilTest {

    @Before
    public void setUp() throws Exception {
        GenericData.Record[] records = {
                recordWithMask("3", 3, bmask(1, 1), new String[]{"a", "b"}),
                recordWithMask("4", 4, bmask(0, 1)),
                recordWithMask("2", 2, bmask(1, 0))
        };

        create("ma", "input.avro", records);

        GenericData.Record[] records2 = {
                recordWithMask("5", 5, bmask(1, 1), new String[]{"b", "c"}),
                recordWithMask("4", 4, bmask(0, 1)),
                recordWithMask("2", 2, bmask(0, 1))
        };

        create("mb", "input.avro", records2);


        GenericData.Record[] records3 = {
                record("5", 5),
                record("6", 6)
        };

        create("d", "input.avro", records3);

        GenericData.Record[] records4 = {
                recordWithExtraField("2", 2, "B"),
                recordWithExtraField("3", 3, null)
        };

        create("e", "input.avro", records4);
    }

    @Test
    public void transpose() {
        Assert.assertArrayEquals(new int[]{2, 0}, MergeJob.computeTranspose(
                new String[]{"a", "b"},
                new String[]{"b", "c", "a"})
        );
    }

    @Test
    public void parseConf() {
        String input = "file:/tmp/junit2081435449853568838/ma|a,b\nfile:/tmp/junit2081435449853568838/mb|b,c";

        Map<String, String[]> parseDirConf = MergeJob.parseDirConf(input);

        Assert.assertTrue(parseDirConf.containsKey("file:/tmp/junit2081435449853568838/ma"));
    }

    @Test
    public void test42() throws Exception {

        String outStr = tmpFolder.getRoot().getPath() + "/out-generic";

        Path diffin = new Path(new File(tmpFolder.getRoot(), "ma").toURI().toString());
        Path diffin2 = new Path(new File(tmpFolder.getRoot(), "mb").toURI().toString());
        Path diffin3 = new Path(new File(tmpFolder.getRoot(), "d").toURI().toString());

        Path output = new Path(outStr);

        Configuration jobConf = new Configuration();
        MergeJob job = new MergeJob();
        Assert.assertTrue(job.internalRun(diffin.toString() + "," + diffin2.toString() + "," + diffin3.toString(), output, jobConf).waitForCompletion(true));

//		Thread.sleep(100000000000l);


        String base = outStr + "/";


        assertContains(base, TestSchema.recordWithMask("3", 3, bmask(1, 1, 0, 0)));
        assertContains(base, TestSchema.recordWithMask("4", 4, bmask(0, 1, 1, 0)));
        assertContains(base, TestSchema.recordWithMask("2", 2, bmask(1, 0, 1, 0)));
        assertContains(base, TestSchema.recordWithMask("6", 6, bmask(0, 0, 0, 1)));
        assertContains(base, TestSchema.recordWithMask("5", 5, bmask(0, 1, 1, 1)));
    }

    @Test
    public void test43() throws Exception {
        String outStr = tmpFolder.getRoot().getPath() + "/out-generic2";

        Path diffin = new Path(new File(tmpFolder.getRoot(), "e").toURI().toString());
        Path diffin2 = new Path(new File(tmpFolder.getRoot(), "ma").toURI().toString());
        Path diffin3 = new Path(new File(tmpFolder.getRoot(), "d").toURI().toString());


        Path output = new Path(outStr);

        Configuration jobConf = new Configuration();
        MergeJob job = new MergeJob();
        Assert.assertTrue(job.internalRun(diffin.toString() + "," + diffin2.toString() + "," + diffin3.toString(), output, jobConf).waitForCompletion(true));


        String base = outStr + "/";

        assertContains(base, TestSchema.recordWithExtraFieldWithMask("2", 2, null, bmask(0, 1, 0, 0)));
        assertContains(base, TestSchema.recordWithExtraFieldWithMask("2", 2, "B", bmask(0, 0, 0, 1)));
        // the next one is tricky, a record from e (with an additionnal field, but at null), should match the same record in ma
        assertContains(base, TestSchema.recordWithExtraFieldWithMask("3", 3, null, bmask(1, 1, 0, 1)));


    }
}
