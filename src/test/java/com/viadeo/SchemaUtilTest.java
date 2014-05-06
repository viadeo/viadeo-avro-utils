package com.viadeo;

import static com.viadeo.SchemaUtils.bmask;

import java.util.Arrays;

import junit.framework.Assert;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class SchemaUtilTest {

    @Test
    public void bytesBitmaskTest() {

        int sizeOfBA = 4;

        Iterable<Text> sides =
                Arrays.asList(new Text[]{
                        new Text(bmask(0, 1, 0, 1)),
                        new Text(bmask(1, 0, 0, 0))
                });

        String results = SchemaUtils.bytesBitmask(sides, sizeOfBA);

        Assert.assertEquals(results, bmask(1, 1, 0, 1));
    }
}
