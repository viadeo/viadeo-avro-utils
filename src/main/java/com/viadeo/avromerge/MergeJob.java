package com.viadeo.avromerge;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.viadeo.SchemaUtils;
import com.viadeo.StringUtils;

public class MergeJob extends Configured implements Tool {

    public static final String DIFFPATHS = "viadeo.diff.diffinpaths";
    public static final String DIRSCONF = "merge.dirsconf";

    public static Map<String, String[]> parseDirConf(String conf) {
        Map<String, String[]> hm = new HashMap<String, String[]>();

        for (String line : conf.split("\n")) {
            String[] lineStruct = line.split("\\|");
            String[] value = (lineStruct.length == 1) ? new String[0] : lineStruct[1].split(",");
            hm.put(lineStruct[0], value);
        }

        return hm;
    }

    public static int[] computeTranspose(String[] inputDirs, String[] jobDirs) {
        List<String> jobDirAsList = Arrays.asList(jobDirs);

        int[] transpose = new int[inputDirs.length];
        for (int i = 0; i < inputDirs.length; i++) {
            String b = inputDirs[i];
            transpose[i] = jobDirAsList.indexOf(b);
        }

        return transpose;
    }


    public static class MergeMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, Text> {

        public String filename;
        public String[] jobDirs;
        public String[] inputDirs;
        public boolean isDiffFile;
        public int indexFile;
        public int[] transpose;

        @Override
        public void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {

        	char[] res = SchemaUtils.initBitmask(jobDirs.length);

            if (isDiffFile) {
                char[] inputmask = ((String) key.datum().get(SchemaUtils.DIFFMASK)).toCharArray();
                //byte[] inputmask = bb.array();

                for (int i = 0; i < inputDirs.length; i++) {
                    res[transpose[i]] = inputmask[i];
                }

            } else {
                res[indexFile] = SchemaUtils.ONE;
            }

            context.write(key, new Text(new String(res)));
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            filename = ((FileSplit) context.getInputSplit()).getPath().getParent().toString() + "/";
            jobDirs = context.getConfiguration().get(MergeJob.DIFFPATHS).split(",");

            Map<String, String[]> hm = parseDirConf(context.getConfiguration().get(DIRSCONF));

            String[] keys = hm.keySet().toArray(new String[0]);
            inputDirs = hm.get(keys[StringUtils.indexOfClosestElement(filename, keys)]);

            isDiffFile = inputDirs.length != 0;
            indexFile = StringUtils.indexOfClosestElement(filename, jobDirs);

            transpose = computeTranspose(inputDirs, jobDirs);

        }
    }

    public static class MergeReducer extends Reducer<AvroKey<GenericData.Record>, Text, AvroKey<GenericData.Record>, NullWritable> {

        private int sizeOfBA;

        @Override
        protected void reduce(AvroKey<GenericData.Record> record, Iterable<Text> sides, Context context) throws IOException, InterruptedException {

            // merge BytesArray
            String bytesMask = SchemaUtils.bytesBitmask(sides, sizeOfBA);

            GenericData.Record datum = record.datum();
            datum.put(SchemaUtils.DIFFMASK, bytesMask);
            context.write(new AvroKey<GenericData.Record>(datum), NullWritable.get());
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            sizeOfBA = context.getConfiguration().get(MergeJob.DIFFPATHS).split(",").length;
        }
    }


    public Job internalRun(String inputDirs, Path outputDir, Configuration conf) throws Exception {

        //conf.set(DIFFPATHS, inputDirs);


        Job job = new Job(conf);
        job.setJarByClass(MergeJob.class);
        job.setJobName("merge");

        String[] dirs = inputDirs.split(",");

        SortedSet<String> deRefDir = new TreeSet<String>();
        StringBuilder dirsConf = new StringBuilder();

        Map<String, Schema.Field> fieldMap = new HashMap<String, Schema.Field>();

        Schema tempS = null;
        for (String dir : dirs) {
            tempS = SchemaUtils.getSchema(conf, new Path(dir));
            if (null != tempS.getField(SchemaUtils.DIFFMASK)) {
                String[] insideDirs = SchemaUtils.getDiffDirs(tempS);
                deRefDir.addAll(Arrays.asList(insideDirs));
                dirsConf.append(dir).append("|").append(StringUtils.mkString(insideDirs, ","));
            } else {
                deRefDir.add(dir);
                dirsConf.append(dir).append("|");
            }
            dirsConf.append("\n");

            for (Schema.Field f : tempS.getFields()) {
                fieldMap.put(f.name(), f);
            }
        }

        job.getConfiguration().set(DIFFPATHS, StringUtils.mkString(deRefDir.toArray(new String[0]), ","));

        job.getConfiguration().set(DIRSCONF, dirsConf.toString());

        Schema outSchema = SchemaUtils.addByteMask(SchemaUtils.constructSchema(tempS, fieldMap), deRefDir.toArray(new String[0]));


        System.out.println(outSchema);


        FileInputFormat.setInputPaths(job, inputDirs);

        job.setMapperClass(MergeMapper.class);
        job.setInputFormatClass(AvroKeyInputFormat.class);
        AvroJob.setInputKeySchema(job, outSchema);

        AvroJob.setMapOutputKeySchema(job, SchemaUtils.ignoreFieldOrder(outSchema, SchemaUtils.DIFFMASK));
        job.setMapOutputValueClass(Text.class);


        job.setReducerClass(MergeReducer.class);
        AvroJob.setOutputKeySchema(job, outSchema);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);


        // ~ OUTPUT
        FileOutputFormat.setOutputPath(job, outputDir);


        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Read <inputdirsCommaSeparated> <output path>");
            for (String arg : args) {
                System.out.println(arg);
            }

            return -1;
        }

        Configuration jobConf = getConf();

        String inputDirs = args[0];
        Path outPath = new Path(args[1]);

        Job job = internalRun(inputDirs, outPath, jobConf);
        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }

        return 0;
    }
}

