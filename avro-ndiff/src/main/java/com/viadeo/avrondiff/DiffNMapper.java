package com.viadeo.avrondiff;


import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class DiffNMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, IntWritable> {


    public static IntWritable indexFile;
    public static Text fileName;

    @Override
    public void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
        context.write(key, indexFile);
    }

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {



        Log log = LogFactory.getLog(DiffNMapper.class);

        String name = ((FileSplit) context.getInputSplit()).getPath().getParent().toString() + "/";

        log.info("--------------------name: " + name);

                  // jobConf.set("viadeo.diff.diffinpath", diffin );
        //jobConf.set("viadeo.diff.diffoutpath", diffout );

        String[] diffins = context.getConfiguration().get(DiffNJob.DIFFPATHS).split(",");




        int minleven = computeLevenshteinDistance(name,diffins[0]);
        int indexleven = 0;
        for(int i = 0; i < diffins.length; i ++) {
            int d = computeLevenshteinDistance(name, diffins[i]);
            if(d < minleven) {
                minleven = d;
                indexleven = i;
            }
        }


        indexFile = new IntWritable(indexleven);

        log.info("------------------------------------------------------" + fileName);

    }

	private static int minimum(int a, int b, int c) {
		return Math.min(Math.min(a, b), c);
	}

	public static int computeLevenshteinDistance(String str1,String str2) {
		int[][] distance = new int[str1.length() + 1][str2.length() + 1];

		for (int i = 0; i <= str1.length(); i++)
			distance[i][0] = i;
		for (int j = 1; j <= str2.length(); j++)
			distance[0][j] = j;

		for (int i = 1; i <= str1.length(); i++)
			for (int j = 1; j <= str2.length(); j++)
				distance[i][j] = minimum(
						distance[i - 1][j] + 1,
						distance[i][j - 1] + 1,
						distance[i - 1][j - 1]+ ((str1.charAt(i - 1) == str2.charAt(j - 1)) ? 0 : 1));

		return distance[str1.length()][str2.length()];
	}

}