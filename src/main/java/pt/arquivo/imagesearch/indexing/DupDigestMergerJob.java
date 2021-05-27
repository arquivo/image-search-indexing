package pt.arquivo.imagesearch.indexing;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
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
import pt.arquivo.imagesearch.indexing.data.ImageData;
import pt.arquivo.imagesearch.indexing.data.MultiPageImageData;
import pt.arquivo.imagesearch.indexing.data.PageImageData;
import pt.arquivo.imagesearch.indexing.data.serializers.ImageDataSerializer;
import pt.arquivo.imagesearch.indexing.data.serializers.MultiPageImageDataSerializer;
import pt.arquivo.imagesearch.indexing.data.serializers.PageImageDataSerializer;
import pt.arquivo.imagesearch.indexing.processors.ImageInformationMerger;

import java.io.IOException;

public class DupDigestMergerJob {

    public enum OUTPUT_MODE {
        FULL,
        COMPACT
    }

    public static final String OUTPUT_MODE_NAME = "output_mode";

    public enum COUNTERS {
        RECORDS_MAP_IN,
        RECORDS_IN,
        RECORDS_OUT,
        RECORDS_WITH_METADATA,
        RECORDS_WITHOUT_METADATA,
        URL_IMAGES_PAGESALL,
        URL_IMAGESALL_PAGES,
        URL_IMAGES_PAGES

    }

    public enum REDUCE_COUNTERS {
        URL_IMAGES_PAGESALL,
        URL_IMAGESALL_PAGES,
        URL_IMAGES_PAGES
    }

    public static class Map extends Mapper<Text, Writable, Text, Writable> {

        private final Logger logger = Logger.getLogger(Map.class);

        public void map(Text key, Writable value, Context context) {
            try {
                context.getCounter(COUNTERS.RECORDS_MAP_IN).increment(1);
                context.write(key, value);
            } catch (IOException | InterruptedException e) {
                logger.error(e.getMessage());
            }
        }
    }

    public static class Reduce extends Reducer<Text, Writable, NullWritable, Text> {

        private final Logger logger = Logger.getLogger(Reduce.class);
        public String collection;
        private ImageInformationMerger merger;
        OUTPUT_MODE outputMode;

        @Override
        public void setup(Reducer.Context context) {
            merger = new ImageInformationMerger(context);
            Configuration config = context.getConfiguration();
            String legacyOutput = config.get(OUTPUT_MODE_NAME);

            outputMode = OUTPUT_MODE.valueOf(legacyOutput);
        }


        public void reduce(Text key, Iterable<Writable> values, Context context) {
            logger.debug("Reducing: " + key);

            merger.reset();
            int counter = merger.mergeAllHadoop(values);
            FullImageMetadata result = merger.getBestMatch();

            merger.getCounter(DupDigestMergerJob.COUNTERS.RECORDS_OUT).increment(1);
            merger.getCounter(DupDigestMergerJob.REDUCE_COUNTERS.URL_IMAGES_PAGESALL).increment(result.getPageImageDatasValues().size());
            merger.getCounter(DupDigestMergerJob.REDUCE_COUNTERS.URL_IMAGESALL_PAGES).increment(result.getImageDatasValues().size());
            merger.getCounter(DupDigestMergerJob.REDUCE_COUNTERS.URL_IMAGES_PAGES).increment(1);

            logger.info(String.format("Found %d records", counter));

            if (result.hasImageMetadata())
                merger.getCounter(DupDigestMergerJob.COUNTERS.RECORDS_WITH_METADATA).increment(1);
            else
                merger.getCounter(DupDigestMergerJob.COUNTERS.RECORDS_WITHOUT_METADATA).increment(1);

            logger.debug("Reducing: " + key);

            exportToJson(context, result);

        }

        private void exportToJson(Reducer<Text, Writable, NullWritable, Text>.Context context, FullImageMetadata result) {
            Gson gson = new GsonBuilder()
                    .registerTypeAdapter(PageImageData.class, new PageImageDataSerializer())
                    .registerTypeAdapter(MultiPageImageData.class, new MultiPageImageDataSerializer())
                    .registerTypeAdapter(ImageData.class, new ImageDataSerializer())
                    .create();

            try {
                if (outputMode == OUTPUT_MODE.FULL) {
                    for (ImageData data : result.getImageDatasValues())
                        context.write(NullWritable.get(), new Text(gson.toJson(data)));
                    for (PageImageData data : result.getPageImageDatasValues())
                        context.write(NullWritable.get(), new Text(gson.toJson(data)));
                } else { // if (outputMode == OUTPUT_MODE.COMPACT) {
                    if (!result.getPageImageDatas().isEmpty() && !result.getImageDatas().isEmpty()){
                        ImageData id = result.getImageDatas().firstKey();
                        MultiPageImageData pid = new MultiPageImageData(result);
                        context.write(NullWritable.get(), new Text(gson.toJson(id)));
                        context.write(NullWritable.get(), new Text(gson.toJson(pid)));
                    }

                }
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

        assert args.length >= 4 : "Missing Output mode (e.g. legacy, full, compact)";
        String outputModeString = args[3];


        Configuration conf = new Configuration();
        conf.set("collection", collection);
        conf.set(OUTPUT_MODE_NAME, outputModeString);

        Job jobDigest = Job.getInstance(conf);
        jobDigest.setJarByClass(DupDigestMergerJob.class);
        jobDigest.setInputFormatClass(SequenceFileInputFormat.class);

        jobDigest.setMapperClass(DupDigestMergerJob.Map.class);
        jobDigest.setMapOutputKeyClass(Text.class);
        jobDigest.setMapOutputValueClass(FullImageMetadata.class);

        jobDigest.setReducerClass(DupDigestMergerJob.Reduce.class);
        jobDigest.setOutputKeyClass(NullWritable.class);
        jobDigest.setOutputValueClass(Text.class);
        jobDigest.setOutputFormatClass(TextOutputFormat.class);

        jobDigest.setJobName(jobName);

        jobDigest.setNumReduceTasks(reducesCount);

        KeyValueTextInputFormat.setInputDirRecursive(jobDigest, true);


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

        inputDir += latestValueLong + "_dups/";

        KeyValueTextInputFormat.setInputDirRecursive(jobDigest, true);
        KeyValueTextInputFormat.addInputPath(jobDigest, new Path(inputDir));

        String outputDirDigest = "/user/amourao/output/" + collection + "/" + latestValueLong + "_nodups/";
        TextOutputFormat.setOutputPath(jobDigest, new Path(outputDirDigest));
        if (hdfs.exists(new Path(outputDirDigest)))
            hdfs.delete(new Path(outputDirDigest), true);

        //job.getConfiguration().setInt("mapreduce.job.running.map.limit", maxMaps); /*Maximum simultaneous maps running*/

        //job.setNumReduceTasks(1);

        //job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", linespermap);
        //job.getConfiguration().setInt("mapreduce.job.running.map.limit", maxMaps); /*Maximum simultaneous maps running*/

        boolean result = result = jobDigest.waitForCompletion(true);

        Counters cn = jobDigest.getCounters();
        CounterGroup counterGroup =cn.getGroup("pt.arquivo.imagesearch.indexing.ImageIndexerWithDupsJob$IMAGE_COUNTERS");
        for (Counter c : counterGroup) {
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        System.out.println("ImageIndexerWithDupsJob$PAGE_COUNTERS");
        counterGroup = cn.getGroup("pt.arquivo.imagesearch.indexing.ImageIndexerWithDupsJob$PAGE_COUNTERS");
        for (Counter c : counterGroup) {
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        System.out.println("ImageIndexerWithDupsJob$REDUCE_COUNTERS");
        counterGroup = cn.getGroup("pt.arquivo.imagesearch.indexing.ImageIndexerWithDupsJob$REDUCE_COUNTERS");
        for (Counter c : counterGroup) {
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        System.out.println("DupDigestMergerJob$COUNTERS");
        cn = jobDigest.getCounters();
        counterGroup = cn.getGroup("pt.arquivo.imagesearch.indexing.DupDigestMergerJob$COUNTERS");
        for (Counter c : counterGroup) {
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        System.out.println("DupDigestMergerJob$REDUCE_COUNTERS");
        cn = jobDigest.getCounters();
        counterGroup = cn.getGroup("pt.arquivo.imagesearch.indexing.DupDigestMergerJob$REDUCE_COUNTERS");
        for (Counter c : counterGroup) {
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        System.exit(result ? 0 : 1);
    }
}