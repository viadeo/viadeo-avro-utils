package com.viadeo.avrodiffstat;

import com.viadeo.AvroUtilsJob;
import com.viadeo.SchemaUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.*;


public class MutationStatJob extends Configured implements Tool {


    final static Schema interschema = Schema.parse("{\"type\":\"record\",\"name\":\"Mutation\",\"fields\":[           \n" +
            " {\"name\":\"fields\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},     \n" +
            " {\"name\":\"diffmaskIn\",\"type\":\"string\"},                 \n" +
            " {\"name\":\"diffmaskOut\",\"type\":\"string\"},{\"name\":\"count\",\"type\":\"int\" , \"default\":0} ]}");


    final static String GROUPBYFIELD = "mutation.group.by.field";

    public static class MutationMapper extends Mapper<AvroKey<GenericRecord>, NullWritable,   Text, AvroValue<GenericRecord>> {


        String fieldName;

        @Override
        public void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
            context.write(new Text( key.datum().get(fieldName).toString()), new AvroValue<GenericRecord>(key.datum()));
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            fieldName = context.getConfiguration().get(GROUPBYFIELD);
        }
    }


    public static class DMComparator implements Comparator<GenericRecord> {
       @Override
        public int compare(GenericRecord genericRecord, GenericRecord genericRecord2) {
            return ((String) genericRecord.get(SchemaUtils.DIFFMASK)).compareTo((String) genericRecord2.get(SchemaUtils.DIFFMASK));
        }
    }


    public static class MutationReducer extends Reducer<Text, AvroValue<GenericRecord>,  AvroKey<GenericRecord>, NullWritable> {

        DMComparator dmComparator = new DMComparator();

        List<String> fields = null;

        @Override
        protected void reduce(Text key, Iterable<AvroValue<GenericRecord>> values, Context context) throws IOException, InterruptedException {

            List<GenericRecord> genericRecords = makeCollection(values);

            Collections.sort(genericRecords, dmComparator);

            for(int i = 0; i + 1 < genericRecords.size(); i ++ ) {



                GenericRecord from = genericRecords.get(i);
                GenericRecord to = genericRecords.get(i+1);


                extractFields(from);

                List<String> mutations = new ArrayList<String>();

                for(String f:fields) {
                    if(!from.get(f).equals(to.get(f))) {
                        mutations.add(f);
                    }
                }

                GenericRecord genericRecord = new GenericData.Record(interschema);

                genericRecord.put("fields", mutations);
                genericRecord.put("diffmaskIn", from.get(SchemaUtils.DIFFMASK));
                genericRecord.put("diffmaskOut", to.get(SchemaUtils.DIFFMASK));
                genericRecord.put("count", 0);


                context.write(new AvroKey<GenericRecord>(genericRecord), NullWritable.get());

            }
        }

        private void extractFields(GenericRecord from) {
            if(fields == null) {
                fields = new LinkedList<String>();
                for(Schema.Field f:from.getSchema().getFields()) {
                    if(!f.name().equals(SchemaUtils.DIFFMASK)) {
                        fields.add(f.name());
                    }
                }
            }
        }


        public static List<GenericRecord> makeCollection(Iterable<AvroValue<GenericRecord>> iter) {
            List<GenericRecord> list = new ArrayList<GenericRecord>();
            for (AvroValue<GenericRecord> item : iter) {
               list.add(new GenericRecordBuilder((GenericData.Record)item.datum()).build());
            }
            return list;
        }
    }







    public Job groupByKeyRun(Path inputDir, Path outputDir, String field, Configuration conf) throws Exception {

        Job job = new Job(conf);
        job.setJarByClass(MutationStatJob.class);
        job.setJobName("mutation-group-by-id");


        job.getConfiguration().set(GROUPBYFIELD, field);

        Schema schema = SchemaUtils.getSchema(conf, inputDir);


        FileInputFormat.setInputPaths(job, inputDir);

        job.setMapperClass(MutationMapper.class);
        // MAPPER INPUT SPEC
        job.setInputFormatClass(AvroKeyInputFormat.class);
        AvroJob.setInputKeySchema(job, schema);

        // MAPPER OUTPUT SPEC + REDUCER INPUT SPEC
        AvroJob.setMapOutputValueSchema(job, schema);
        job.setMapOutputKeyClass(Text.class);


        job.setReducerClass(MutationReducer.class);
        // REDUCER OUTPUT SPEC

        /**
         * {"type":"record","name":"Mutation","fields":[
         *  {"name":"fields","type":"array","items":"string"},
         *  {"name":"diffmaskIn","type":"string"},
         *  {"name":"diffmaskOut","type":"string"}]}

         */

        AvroJob.setOutputKeySchema(job, interschema);

        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);

        // ~ OUTPUT
        FileOutputFormat.setOutputPath(job, outputDir);

        return job;
    }


    public static class AccMapper extends Mapper<AvroKey<GenericRecord>, NullWritable,   AvroKey<GenericRecord>, IntWritable> {
        @Override
        protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
            context.write(key, new IntWritable(1));
        }
    }

    public static class AccCombiner extends Reducer<AvroKey<GenericRecord>,IntWritable, AvroKey<GenericRecord>,IntWritable> {
        @Override
        protected void reduce(AvroKey<GenericRecord> key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int c = 0;
            for(IntWritable v:values) {
                c += v.get();
            }

            context.write(key,new IntWritable(c));
        }
    }

    public static class AccReducer extends  Reducer<AvroKey<GenericRecord>,IntWritable, AvroKey<GenericRecord>, NullWritable> {

        @Override
        protected void reduce(AvroKey<GenericRecord> key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            GenericRecord datum = key.datum();
            int c = 0;

            for(IntWritable i:values) {
                c += i.get();
            }

            datum.put("count", c);
            context.write(new AvroKey<GenericRecord>(datum), NullWritable.get());
        }
    }



    public Job accByResult(Path inputdir, Path outputdir, Configuration conf) throws Exception {

        Job job = new Job(conf);
        job.setJarByClass(MutationStatJob.class);
        job.setJobName("mutation-acc");

        Schema schema = SchemaUtils.getSchema(conf, inputdir);


        FileInputFormat.setInputPaths(job, inputdir);

        job.setMapperClass(AccMapper.class);
        // MAPPER INPUT SPEC
        job.setInputFormatClass(AvroKeyInputFormat.class);
        AvroJob.setInputKeySchema(job, schema);

        AvroJob.setMapOutputKeySchema(job, schema);

        job.setMapOutputValueClass(IntWritable.class);
        job.setCombinerClass(AccCombiner.class);


        job.setReducerClass(AccReducer.class);

        AvroJob.setOutputKeySchema(job,interschema );
        job.setOutputValueClass(NullWritable.class);



        job.setOutputFormatClass(AvroKeyOutputFormat.class);

        // ~ OUTPUT
        FileOutputFormat.setOutputPath(job, outputdir);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: Read <input path> <output path> <field>");
            return -1;
        }

        Configuration jobConf = getConf();

        Path origPath = new Path(args[0]);
        Path outPath = new Path(args[1]);

        String field = args[2];

        Path tempBetween = AvroUtilsJob.tempDirectory();

        Path tempOut = AvroUtilsJob.tempDirectory();
        FileSystem fileSystem = FileSystem.get(jobConf);
        if(fileSystem.exists(outPath)) {
            throw new Exception( "output Path already exist : " + outPath);
        }


        Job job = groupByKeyRun(origPath, tempBetween, field , jobConf);

        boolean b = job.waitForCompletion(true);

        if(b) {

            Job job2 = accByResult(tempBetween, tempOut, jobConf);

            return AvroUtilsJob.runAndWatchJobThenMv(job2, tempOut, outPath);
        }

        throw new RuntimeException("ERROR");


    }
}
