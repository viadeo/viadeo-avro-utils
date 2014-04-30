package com.viadeo;

import static com.viadeo.SchemaUtils.bmask;

import java.util.Arrays;

import junit.framework.Assert;

import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

public class SchemaUtilTest {

	@Test
	public void bytesBitmaskTest(){

		int sizeOfBA = 4;

		new BytesWritable(bmask(0,1,0,1));

		Iterable<BytesWritable> sides =
				Arrays.asList(new BytesWritable[] {
						new BytesWritable(bmask(0,1,0,1)),
						new BytesWritable(bmask(1,0,0,0))
						});

		byte[] results = SchemaUtils.bytesBitmask(sides, sizeOfBA);

		Assert.assertTrue(Arrays.equals(results, bmask(1,1,0,1)));
	}


}
