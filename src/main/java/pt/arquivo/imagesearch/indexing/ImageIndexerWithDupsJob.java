package pt.arquivo.imagesearch.indexing;

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
import pt.arquivo.imagesearch.indexing.processors.ImageInformationExtractor;
import pt.arquivo.imagesearch.indexing.processors.ImageInformationMerger;
import pt.arquivo.imagesearch.indexing.utils.ImageSearchIndexingUtil;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

public class ImageIndexerWithDupsJob {

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

        PAGE_UTF8_MISMATCH,
        PAGE_UTF8_MISMATCH_DOUBLE,

        IMAGES_IN_HTML_SENT,
        IMAGES_IN_HTML_SENT_DUP,
    }

    public enum REDUCE_COUNTERS {
        URL_IMAGES_PAGES,
        URL_IMAGES_PAGESALL,
        URL_IMAGESALL_PAGES,
        URL_IMAGES_NPAGES,
        URL_IMAGESALL_NPAGES,
        URL_NIMAGES_PAGES,
        URL_NIMAGES_PAGESALL,
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

        public void map(LongWritable key, Text value, Context context) throws IOException {
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
                String arcName = surl[surl.length - 1];
                String filename = "/tmp/" + System.currentTimeMillis() + "_" + arcName;
                //

                try {
                    int fileSize = ImageSearchIndexingUtil.getFileSize(url);
                    //ImageSearchIndexingUtil.saveFile(url, filename);
                    File dest = new File( filename);
                    FileUtils.copyURLToFile(url, dest, 1000*60, 1000*30);
                    dest = new File( filename);
                    if (fileSize != dest.length()){
                        FileUtils.deleteQuietly(dest);
                        throw new IOException("Incomplete file: " + fileSize +  " " + dest.length());
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    context.getCounter(IMAGE_COUNTERS.WARCS_DOWNLOAD_ERROR).increment(1);
                    File dest = new File( filename);
                    FileUtils.deleteQuietly(dest);
                    throw e;
                }
                File dest = new File( filename);
                logger.info("(W)ARC downloaded: " + dest.getAbsolutePath());

                indexer.parseRecord(arcName, dest.getPath());
                FileUtils.deleteQuietly(dest);

            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            logger.info("Cleanup");
            super.cleanup(context);
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
            merger.reset();
            int counter = merger.mergeAllHadoop(values);
            FullImageMetadata result = merger.getBestMatch();
            logger.debug(String.format("Found %d pages and %d images", result.getPageImageDatasValues().size(), result.getImageDatasValues().size()));

            if (result.getImageDatasValues().size() != 0 && result.getPageImageDatasValues().size() != 0) {
                merger.getCounter(ImageIndexerWithDupsJob.REDUCE_COUNTERS.URL_IMAGES_PAGESALL).increment(result.getPageImageDatasValues().size());
                merger.getCounter(ImageIndexerWithDupsJob.REDUCE_COUNTERS.URL_IMAGESALL_PAGES).increment(result.getImageDatasValues().size());
                merger.getCounter(ImageIndexerWithDupsJob.REDUCE_COUNTERS.URL_IMAGES_PAGES).increment(1);
                //logger.debug(String.format("%s: Found %d images and %d pages; image TS: \"%s\" page TS: \"%s\"", key, images.size(), pages.size(), images.get(0) == null ? "none" : images.get(0).getTimestamp().toString(), pages.get(0) == null ? "none" : pages.get(0).getTimestamp().toString()));
                try {
                    Set<String> digests = new HashSet<>();
                    for (ImageData imageData : result.getImageDatasValues()) {
                        String digest = imageData.getContentHash();
                        merger.getCounter(REDUCE_COUNTERS.URL_IMAGES_PAGES_DIGESTALL).increment(1);
                        if (!digests.contains(imageData.getContentHash())) {
                            FullImageMetadata resultDigest = new FullImageMetadata(result, imageData);
                            context.write(new Text(digest), resultDigest);
                            digests.add(digest);
                            merger.getCounter(REDUCE_COUNTERS.URL_IMAGES_PAGES_MULIPLE_DIGEST).increment(1);
                        }
                    }
                } catch (IOException | InterruptedException e) {
                    logger.error(e.getMessage());
                }
            } else if (result.getImageDatasValues().size() != 0) {
                merger.getCounter(ImageIndexerWithDupsJob.REDUCE_COUNTERS.URL_IMAGES_NPAGES).increment(1);
                merger.getCounter(ImageIndexerWithDupsJob.REDUCE_COUNTERS.URL_IMAGESALL_NPAGES).increment(result.getImageDatasValues().size());
            } else if (result.getPageImageDatasValues().size() != 0) {
                merger.getCounter(ImageIndexerWithDupsJob.REDUCE_COUNTERS.URL_NIMAGES_PAGES).increment(1);
                merger.getCounter(ImageIndexerWithDupsJob.REDUCE_COUNTERS.URL_NIMAGES_PAGESALL).increment(result.getPageImageDatasValues().size());
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
        job.setJarByClass(ImageIndexerWithDupsJob.class);
        job.setInputFormatClass(NLineInputFormat.class);

        job.setMapperClass(ImageIndexerWithDupsJob.Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FullImageMetadata.class);

        job.setReducerClass(ImageIndexerWithDupsJob.Reduce.class);
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

        System.out.println("ImageIndexerWithDupsJob$IMAGE_COUNTERS");
        Counters cn = job.getCounters();
        CounterGroup counterGroup = cn.getGroup("pt.arquivo.imagesearch.indexing.ImageIndexerWithDupsJob$IMAGE_COUNTERS");
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


        System.exit(result ? 0 : 1);
    }
}