import com.google.gson.Gson;

import data.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

public class ImageIndexerWithDups {

    public enum IMAGE_COUNTERS {
        WARCS,

        WARCS_FAILED,

        WARCS_FAILED_STREAM,

        RECORDS_READ,
        RECORDS_FAILED,
        RECORD_NEXT_FAILED,

        IMAGES_IN_WARC_TOTAL,
        IMAGES_IN_WARC_FAILED,

        IMAGES_IN_WARC_TOO_SMALL,
        IMAGES_IN_WARC_TOO_SMALL_BASE64,
        IMAGES_IN_WARC_TOO_LARGE,
        IMAGES_IN_WARC_MIME_INVALID,
        IMAGES_IN_WARC_MIME_WRONG,

        IMAGES_IN_WARC_PARSED,
        IMAGES_IN_WARC_PARSED_DUP

    }

    public enum PAGE_COUNTERS {
        IMAGES_IN_HTML_TOTAL,
        IMAGES_IN_HTML_FAILED,
        IMAGES_IN_HTML_INVALID,
        IMAGES_IN_HTML_MATCHING,
        IMAGES_IN_HTML_BASE64,
        PAGES,
        PAGES_WITH_IMAGES,

        IMAGES_IN_HTML_SENT,
        IMAGES_IN_HTML_SENT_METADATA_UPDATED,
        IMAGES_IN_HTML_METADATA_CHANGED,
        IMAGES_IN_HTML_SENT_DUP,

    }

    public enum REDUCE_COUNTERS {
        URL_IMAGES_PAGES,
        URL_IMAGES_PAGES_WITH_METADATA,
        URL_IMAGES_PAGES_WITHOUT_METADATA,
        URL_IMAGES_PAGESALL,
        URL_IMAGESALL_PAGES,
        URL_IMAGES_NPAGES,
        URL_IMAGESALL_NPAGES,
        URL_NIMAGES_PAGES,
        URL_NIMAGES_PAGESALL,
        IMAGES_PAGES_EXCEEDED
    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private final Logger logger = Logger.getLogger(Map.class);
        public String collection;
        ImageInformationExtractor indexer;


        @Override
        public void setup(Context context) {
            //logger.setLevel(Level.DEBUG);
            Configuration config = context.getConfiguration();
            collection = config.get("collection");
            logger.debug(collection + "_Images/img/");
            this.collection = config.get("collection");
            indexer = new ImageInformationExtractor(collection, context);
        }

        public void map(LongWritable key, Text value, Context context) {
            String arcURL = value.toString();
            if (!arcURL.isEmpty()) {
                logger.info("(W)ARCNAME: " + arcURL);
                context.getCounter(IMAGE_COUNTERS.WARCS).increment(1);
                indexer.parseRecord(arcURL);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            Gson gson = new Gson();
            for (java.util.Map.Entry<String, PageImageData> entry : indexer.getImgSrcEntries().entrySet()) {
                String surt = entry.getKey();
                context.write(new Text(surt), new Text(gson.toJson(entry.getValue())));
            }

            for (java.util.Map.Entry<String, ImageData> entry : indexer.getImgFileEntries().entrySet()) {
                String surt = entry.getKey();
                context.write(new Text(surt), new Text(gson.toJson(entry.getValue())));
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        private final Logger logger = Logger.getLogger(Reduce.class);
        public String collection;
        ImageInformationMerger merger;

        @Override
        public void setup(Reducer.Context context) {
            //logger.setLevel(Level.DEBUG);
            Configuration config = context.getConfiguration();
            collection = config.get("collection");

            logger.debug(collection + "_Images" + "/img/");
            this.collection = config.get("collection");

            merger = new ImageInformationMerger(context);

        }


        public void reduce(Text key, Iterable<Text> values, Context context) {
            Gson gson = new Gson();
            int counter = 0;

            merger.reset();
            //TODO: check http://codingjunkie.net/secondary-sort  to see if it helps not having to iterate all records
            logger.debug("Reducing: " + key);
            for (Text val : values) {
                merger.add(val);
                counter++;
                if (counter >= 1000) {
                    logger.info(String.format("Broke iterating: %d pages and %d images", merger.getPages().size(), merger.getImages().size()));
                    merger.getCounter(ImageIndexerWithDups.REDUCE_COUNTERS.IMAGES_PAGES_EXCEEDED).increment(1);
                    break;
                }

            }
            logger.debug(String.format("Found %d pages and %d images", merger.getPages().size(), merger.getImages().size()));

            if (merger.getImages().size() != 0 && merger.getPages().size() != 0) {

                merger.getCounter(REDUCE_COUNTERS.URL_IMAGES_PAGESALL).increment(merger.getPages().size());
                merger.getCounter(REDUCE_COUNTERS.URL_IMAGESALL_PAGES).increment(merger.getImages().size());
                merger.getCounter(REDUCE_COUNTERS.URL_IMAGES_PAGES).increment(1);

                //logger.debug(String.format("%s: Found %d images and %d pages; image TS: \"%s\" page TS: \"%s\"", key, images.size(), pages.size(), images.get(0) == null ? "none" : images.get(0).getTimestamp().toString(), pages.get(0) == null ? "none" : pages.get(0).getTimestamp().toString()));

                try {
                    FullImageMetadata fim = merger.getBestMatch();
                    for(String digest: fim.getImgDigests()){
                        context.write(new Text(digest), new Text(gson.toJson(fim)));
                    }
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            } else if (merger.getImages().size() != 0) {
                merger.getCounter(REDUCE_COUNTERS.URL_IMAGES_NPAGES).increment(1);
                merger.getCounter(REDUCE_COUNTERS.URL_IMAGESALL_NPAGES).increment(merger.getImages().size());
            } else if (merger.getPages().size() != 0) {
                merger.getCounter(REDUCE_COUNTERS.URL_NIMAGES_PAGES).increment(1);
                merger.getCounter(REDUCE_COUNTERS.URL_NIMAGES_PAGESALL).increment(merger.getPages().size());
            }

        }


    }

    public static void main(String[] args) throws Exception {
        assert args.length >= 1 : "Missing hdfs file with all arcs path argument";
        String hdfsArcsPath = args[0];

        assert args.length >= 2 : "Missing collection name argument";
        String collection = args[1];
        String jobName = collection + "_FullIndexer";

        assert args.length >= 3 : "Missing number of warcs per map";
        int linesPerMap = Integer.parseInt(args[2]);

        assert args.length >= 4 : "Missing number of reduces";
        int reducesCount = Integer.parseInt(args[3]);


        Configuration conf = new Configuration();
        conf.set("collection", collection);

        Job job = Job.getInstance(conf);
        job.setJarByClass(ImageIndexerWithDups.class);
        job.setInputFormatClass(NLineInputFormat.class);

        job.setMapperClass(ImageIndexerWithDups.Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(ImageIndexerWithDups.Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setJobName(jobName);

        NLineInputFormat.addInputPath(job, new Path(hdfsArcsPath));


        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", linesPerMap);
        //job.getConfiguration().setInt("mapreduce.job.running.map.limit", maxMaps); /*Maximum simultaneous maps running*/
        // Sets reducer tasks to 1
        job.setNumReduceTasks(reducesCount);
        //job.setNumReduceTasks(1);

        //job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", linespermap);
        //job.getConfiguration().setInt("mapreduce.job.running.map.limit", maxMaps); /*Maximum simultaneous maps running*/

        String outputDir = "/user/amourao/output/" + collection + "/" + System.currentTimeMillis();
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(new Path(outputDir)))
            hdfs.delete(new Path(outputDir), true);

        boolean result = job.waitForCompletion(true);

        System.out.println("ImageIndexerWithDups$IMAGE_COUNTERS");
        Counters cn = job.getCounters();
        CounterGroup counterGroup = cn.getGroup("ImageIndexerWithDups$IMAGE_COUNTERS");
        for (Counter c : counterGroup) {
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        System.out.println("ImageIndexerWithDups$PAGE_COUNTERS");
        counterGroup = cn.getGroup("ImageIndexerWithDups$PAGE_COUNTERS");
        for (Counter c : counterGroup) {
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        System.out.println("ImageIndexerWithDups$REDUCE_COUNTERS");
        counterGroup = cn.getGroup("ImageIndexerWithDups$REDUCE_COUNTERS");
        for (Counter c : counterGroup) {
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        System.exit(result ? 0 : 1);
    }
}