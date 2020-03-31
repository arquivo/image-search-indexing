package pt.arquivo.imagesearch.indexing;

import com.google.gson.Gson;
import pt.arquivo.imagesearch.indexing.data.FullImageMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

public class DupDigestMergerJob {

    public enum COUNTERS {
        RECORDS_EXCEEDED,
        RECORDS_MAP_IN,
        RECORDS_IN,
        RECORDS_OUT,
        RECORDS_WITH_METADATA,
        RECORDS_WITHOUT_METADATA

    }

    public static class Map extends Mapper<Text, Text, Text, Text> {

        private final Logger logger = Logger.getLogger(Map.class);

        public void map(Text key, Text value, Context context) {
            try {
                context.getCounter(COUNTERS.RECORDS_MAP_IN).increment(1);
                context.write(key, value);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, NullWritable, Text> {

        private final Logger logger = Logger.getLogger(Reduce.class);
        public String collection;
        private DupDigestMerger merger;

        @Override
        public void setup(Reducer.Context context) {
            merger = new DupDigestMerger();
        }


        public void reduce(Text key, Iterable<Text> values, Context context) {
            int counter = 0;
            Gson gson = new Gson();
            FullImageMetadata result = null;

            for (Text val : values) {
                //RECORDS_MAP may not match RECORDS_MAP_IN due to the RECORDS_EXCEEDED breaking very large entries
                context.getCounter(COUNTERS.RECORDS_IN).increment(1);
                FullImageMetadata metadata = merger.parseRecord(val);
                if (result == null) {
                    result = metadata;
                } else {
                    result.merge(metadata);
                }

                //TODO: check behaviour in FAWP. There are many more duplicates here
                if (counter >= 1000) {
                    logger.info(String.format("Broke iterating: %d records", counter));
                    context.getCounter(COUNTERS.RECORDS_EXCEEDED).increment(1);
                    break;
                }
                counter++;

            }

            if (result.hasImageMetadata())
                context.getCounter(COUNTERS.RECORDS_WITH_METADATA).increment(1);
            else
                context.getCounter(COUNTERS.RECORDS_WITHOUT_METADATA).increment(1);

            context.getCounter(COUNTERS.RECORDS_OUT).increment(1);
            logger.info(String.format("Found %d records", counter));

            try {
                context.write(NullWritable.get(), new Text(gson.toJson(result)));
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }

        }


    }

    public static void main(String[] args) throws Exception {
        assert args.length >= 1 : "Missing collection name argument";
        String collection = args[0];
        String jobName = collection + "_DupDigestMergerJob";

        assert args.length >= 2 : "Missing number of files per map";
        int linesPerMap = Integer.parseInt(args[1]);

        assert args.length >= 3 : "Missing number of reduces";
        int reducesCount = Integer.parseInt(args[2]);


        Configuration conf = new Configuration();
        conf.set("collection", collection);

        Job jobDigest = Job.getInstance(conf);
        jobDigest.setJarByClass(DupDigestMergerJob.class);
        jobDigest.setInputFormatClass(KeyValueTextInputFormat.class);

        jobDigest.setMapperClass(DupDigestMergerJob.Map.class);
        jobDigest.setMapOutputKeyClass(Text.class);
        jobDigest.setMapOutputValueClass(Text.class);

        jobDigest.setReducerClass(DupDigestMergerJob.Reduce.class);
        jobDigest.setOutputKeyClass(NullWritable.class);
        jobDigest.setOutputValueClass(Text.class);
        jobDigest.setOutputFormatClass(TextOutputFormat.class);

        jobDigest.setJobName(jobName);

        jobDigest.setNumReduceTasks(reducesCount);

        String inputDir = "/user/amourao/output/" + collection + "/";

        FileSystem hdfs = FileSystem.get(conf);
        FileStatus[] fileStatus = hdfs.listStatus(new Path(inputDir));


        long latestValueLong = 0;
        for (FileStatus fileStat : fileStatus) {
            if (fileStat.isDir()) {
                if (fileStat.getPath().getName().endsWith("_dups")) {
                    try {
                        String name = fileStat.getPath().getName().replace("_dups", "");
                        long currentValueLong = Long.parseLong(name);
                        if (currentValueLong > latestValueLong)
                            latestValueLong = currentValueLong;
                    } catch (Exception ignore) {

                    }
                }
            }
        }

        inputDir += latestValueLong + "/";
        KeyValueTextInputFormat.setInputDirRecursive(jobDigest, true);
        KeyValueTextInputFormat.addInputPath(jobDigest, new Path(inputDir));

        String outputDir = "/user/amourao/output/" + collection + "/" + latestValueLong + "_nodups/";
        TextOutputFormat.setOutputPath(jobDigest, new Path(outputDir));
        if (hdfs.exists(new Path(outputDir)))
            hdfs.delete(new Path(outputDir), true);

        //job.getConfiguration().setInt("mapreduce.job.running.map.limit", maxMaps); /*Maximum simultaneous maps running*/

        //job.setNumReduceTasks(1);

        //job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", linespermap);
        //job.getConfiguration().setInt("mapreduce.job.running.map.limit", maxMaps); /*Maximum simultaneous maps running*/

        boolean result = jobDigest.waitForCompletion(true);

        System.out.println("DupDigestMergerJob$COUNTERS");
        Counters cn = jobDigest.getCounters();
        CounterGroup counterGroup = cn.getGroup("pt.arquivo.imagesearch.indexing.DupDigestMergerJob$COUNTERS");
        for (Counter c : counterGroup) {
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        System.exit(result ? 0 : 1);
    }
}