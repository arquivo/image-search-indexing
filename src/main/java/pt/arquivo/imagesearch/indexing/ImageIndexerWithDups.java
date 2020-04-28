package pt.arquivo.imagesearch.indexing;

import com.google.gson.Gson;

import org.apache.hadoop.io.Writable;
import pt.arquivo.imagesearch.indexing.data.*;
import org.apache.commons.io.FileUtils;
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

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

import static pt.arquivo.imagesearch.indexing.DupDigestMerger.parseRecord;

public class ImageIndexerWithDups {

    public enum IMAGE_COUNTERS {
        WARCS,
        WARCS_DOWNLOAD_ERROR,

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
        IMAGES_IN_HTML_EXCEDED,
        IMAGES_IN_HTML_NOT_PARSED,
        IMAGES_IN_HTML_MATCHING_ALT_ATRIB,
        IMAGES_IN_HTML_MATCHING_LINK,
        IMAGES_IN_HTML_MATCHING_CSS,
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
        IMAGES_PAGES_EXCEEDED,
        URL_IMAGES_PAGES_DIGESTALL,
        URL_IMAGES_PAGES_MULIPLE_DIGEST
    }

    public static class Map extends Mapper<LongWritable, Text, Text, Writable> {

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

                URL url = null;
                try {
                    url = new URL(arcURL);
                } catch (MalformedURLException ignored) {

                }
                String[] surl = url.getPath().split("/");
                String filename = System.currentTimeMillis() + "_" + surl[surl.length - 1];
                File dest = new File("/tmp/" + filename);

                try {
                    FileUtils.copyURLToFile(url, dest);
                } catch (IOException e) {
                    e.printStackTrace();
                    context.getCounter(IMAGE_COUNTERS.WARCS_DOWNLOAD_ERROR).increment(1);
                }

                indexer.parseRecord(dest.getPath());
                FileUtils.deleteQuietly(dest);

            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            Gson gson = new Gson();
            for (java.util.Map.Entry<String, FullImageMetadata> entry : indexer.getEntries().entrySet()) {
                String surt = entry.getKey();
                context.write(new Text(surt), entry.getValue());
            }
        }
    }

    public static class Reduce extends Reducer<Text, Writable, Text, Writable> {

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


        public void reduce(Text key, Iterable<Writable> values, Context context) {
            int counter = 0;

            merger.reset();
            FullImageMetadata result = null;
            //TODO: check http://codingjunkie.net/secondary-sort  to see if it helps not having to iterate all records
            logger.debug("Reducing: " + key);
            for (Writable val : values) {
                merger.merge((FullImageMetadata) val);
                counter++;

                //TODO: check behaviour in FAWP. There are many more duplicates here
                if (counter >= 1000) {
                    logger.info(String.format("Broke iterating: Found %d pages and %d images", result.getPageImageDatasValues().size(), result.getImageDatasValues().size()));
                    merger.getCounter(ImageIndexerWithDups.REDUCE_COUNTERS.IMAGES_PAGES_EXCEEDED).increment(1);
                    break;
                }

            }
            result = merger.getBestMatch();

            logger.debug(String.format("Found %d pages and %d images", result.getPageImageDatasValues().size(), result.getImageDatasValues().size()));


            if (result.getImageDatasValues().size() != 0 && result.getPageImageDatasValues().size() != 0) {

                merger.getCounter(ImageIndexerWithDups.REDUCE_COUNTERS.URL_IMAGES_PAGESALL).increment(result.getPageImageDatasValues().size());
                merger.getCounter(ImageIndexerWithDups.REDUCE_COUNTERS.URL_IMAGESALL_PAGES).increment(result.getImageDatasValues().size());
                merger.getCounter(ImageIndexerWithDups.REDUCE_COUNTERS.URL_IMAGES_PAGES).increment(1);

                //logger.debug(String.format("%s: Found %d images and %d pages; image TS: \"%s\" page TS: \"%s\"", key, images.size(), pages.size(), images.get(0) == null ? "none" : images.get(0).getTimestamp().toString(), pages.get(0) == null ? "none" : pages.get(0).getTimestamp().toString()));

                if (result.getImageDatasValues().size() != 0) {
                    merger.getCounter(ImageIndexerWithDups.REDUCE_COUNTERS.URL_IMAGES_NPAGES).increment(1);
                    merger.getCounter(ImageIndexerWithDups.REDUCE_COUNTERS.URL_IMAGESALL_NPAGES).increment(result.getImageDatasValues().size());
                } else if (result.getPageImageDatasValues().size() != 0) {
                    merger.getCounter(ImageIndexerWithDups.REDUCE_COUNTERS.URL_NIMAGES_PAGES).increment(1);
                    merger.getCounter(ImageIndexerWithDups.REDUCE_COUNTERS.URL_NIMAGES_PAGESALL).increment(result.getPageImageDatasValues().size());
                }

                try {
                    Set<String> digests = new HashSet<>();
                    for (ImageData imageData : result.getImageDatasValues()) {
                        String digest = imageData.getContentHash();
                        merger.getCounter(REDUCE_COUNTERS.URL_IMAGES_PAGES_DIGESTALL).increment(1);
                        context.write(new Text(digest), result);
                        digests.add(digest);
                        if (digests.size() > 1)
                            merger.getCounter(REDUCE_COUNTERS.URL_IMAGES_PAGES_MULIPLE_DIGEST).increment(1);
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }


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
        job.setMapOutputValueClass(FullImageMetadata.class);

        job.setReducerClass(ImageIndexerWithDups.Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FullImageMetadata.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setJobName(jobName);

        NLineInputFormat.addInputPath(job, new Path(hdfsArcsPath));


        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", linesPerMap);
        job.getConfiguration().setFloat("mapreduce.job.reduce.slowstart.completedmaps", 0.9f);

        //job.getConfiguration().setInt("mapreduce.job.running.map.limit", maxMaps); /*Maximum simultaneous maps running*/
        // Sets reducer tasks to 1
        job.setNumReduceTasks(reducesCount);
        //job.setNumReduceTasks(1);

        //job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", linespermap);
        //job.getConfiguration().setInt("mapreduce.job.running.map.limit", maxMaps); /*Maximum simultaneous maps running*/

        String outputDir = "/user/amourao/output/" + collection + "/" + System.currentTimeMillis() + "_dups";
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(new Path(outputDir)))
            hdfs.delete(new Path(outputDir), true);

        boolean result = job.waitForCompletion(true);

        System.out.println("ImageIndexerWithDups$IMAGE_COUNTERS");
        Counters cn = job.getCounters();
        CounterGroup counterGroup = cn.getGroup("pt.arquivo.imagesearch.indexing.ImageIndexerWithDups$IMAGE_COUNTERS");
        for (Counter c : counterGroup) {
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        System.out.println("ImageIndexerWithDups$PAGE_COUNTERS");
        counterGroup = cn.getGroup("pt.arquivo.imagesearch.indexing.ImageIndexerWithDups$PAGE_COUNTERS");
        for (Counter c : counterGroup) {
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        System.out.println("ImageIndexerWithDups$REDUCE_COUNTERS");
        counterGroup = cn.getGroup("pt.arquivo.imagesearch.indexing.ImageIndexerWithDups$REDUCE_COUNTERS");
        for (Counter c : counterGroup) {
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        System.exit(result ? 0 : 1);
    }
}