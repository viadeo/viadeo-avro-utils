package com.viadeo.avromergetodate;

import com.viadeo.AvroUtilsJob;
import com.viadeo.SchemaUtils;
import com.viadeo.StringUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;


public class MergeToDateJob extends Configured implements Tool {

    public static final String DIFFDATES = "viadeo.diff.dates";

    public static final String dm_from_datetime = "dm_from_datetime";
    public static final String dm_to_datetime = "dm_to_datetime";




    public static class ToDateMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, NullWritable> {

        private long[] dates;

        @Override
        public void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
            GenericRecord datum = key.datum();
            char[] bytemask = ((String) datum.get(SchemaUtils.DIFFMASK)).toCharArray();

            int minpos = -1;
            int maxpos = -1;
            for(int i  = 0; i<bytemask.length; i++) {
                if(bytemask[i] == SchemaUtils.ONE) {
                    if(minpos == -1) {
                        minpos =i;
                    }
                    maxpos = i;
                }
            }

            GenericData.Record build = new GenericRecordBuilder((GenericData.Record) datum).build();




            build.put(dm_from_datetime, dates[minpos == 0 ? 0 : minpos - 1]);
            build.put(dm_to_datetime, dates[maxpos]);

            context.write(new AvroKey<GenericRecord>(build), NullWritable.get());
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String s = context.getConfiguration().get(DIFFDATES);

            String[] split = s.split(",");

            dates = new long[split.length];
            for(int i= 0;i<split.length;i++) {
                dates[i] = Long.valueOf(split[i]);
            }


            Log log = LogFactory.getLog(ToDateMapper.class);

            log.info(Arrays.toString(dates));

        }
    }


    public String propToTime(String prop) {
        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

        String[] split = prop.split(",");

        List<String> sb = new ArrayList<String>();
        for (String dir : split) {
            for (String part : dir.split("/")) {
                if (part.contains("=")) {
                    String[] split1 = part.split("=");
                    String key = split1[0];
                    String value = split1[1];

                    if (key.equals("importdatetime")) {

                        //2014-01-25T00_31_49p01_00
                        String s = value.replaceAll("_", ":").replaceAll("p", "+");

                        int index = s.length() - 3;
                        s = s.substring(0, index) + s.substring(index+1);

                        Date parse = null;
                        try {
                            parse = simpleDateFormat.parse(s);
                        } catch (ParseException e) {
                            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                        }

                        sb.add(Long.toString(parse.getTime()));
                    }
                }
            }
        }

        return StringUtils.mkString(sb,",");
    }


    public Job internalRun(Path inputDir, Path outputDir, Configuration conf) throws Exception {

        Job job = new Job(conf);
        job.setJarByClass(MergeToDateJob.class);
        job.setJobName("merge-to-date");

        Schema schema = SchemaUtils.getSchema(conf, inputDir);

        //TODO : assure it exist
        Schema.Field diffField = schema.getField(SchemaUtils.DIFFMASK);
        String prop = diffField.getProp(SchemaUtils.DIFFDIRSPROPNAME);



        job.getConfiguration().set(DIFFDATES, propToTime(prop));

        schema = SchemaUtils.addField(schema, "{\"name\":\"dm_from_datetime\",\"type\":\"long\",\"default\":0}");
        schema = SchemaUtils.addField(schema,"{\"name\":\"dm_to_datetime\",\"type\":\"long\",\"default\":0}");


        FileInputFormat.setInputPaths(job, inputDir);

        job.setMapperClass(ToDateMapper.class);
        // MAPPER INPUT SPEC
        job.setInputFormatClass(AvroKeyInputFormat.class);
        AvroJob.setInputKeySchema(job, schema);

        // MAPPER OUTPUT SPEC + REDUCER INPUT SPEC
        AvroJob.setMapOutputKeySchema(job, schema);
        job.setMapOutputValueClass(NullWritable.class);




        job.setReducerClass(Reducer.class);
        // REDUCER OUTPUT SPEC
        AvroJob.setOutputKeySchema(job, schema);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);

        // ~ OUTPUT
        FileOutputFormat.setOutputPath(job, outputDir);


        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Read <input path> <output path>");
            return -1;
        }

        Configuration jobConf = getConf();

        Path origPath = new Path(args[0]);
        Path outPath = new Path(args[1]);

        Path tempOut = AvroUtilsJob.tempDirectory();
        FileSystem fileSystem = FileSystem.get(jobConf);
        if(fileSystem.exists(outPath)) {
            throw new Exception( "output Path already exist : " + outPath);
        }


        Job job = internalRun(origPath, tempOut , jobConf);

        return AvroUtilsJob.runAndWatchJobThenMv(job, tempOut, outPath);
    }
}
