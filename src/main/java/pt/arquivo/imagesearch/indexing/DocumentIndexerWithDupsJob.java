package pt.arquivo.imagesearch.indexing;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import pt.arquivo.imagesearch.indexing.data.*;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import pt.arquivo.imagesearch.indexing.data.hadoop.ArchiveFileInputFormat;
import pt.arquivo.imagesearch.indexing.data.serializers.TextDocumentDataSerializer;
import pt.arquivo.imagesearch.indexing.processors.DocumentInformationExtractor;
import pt.arquivo.imagesearch.indexing.utils.AlternativeFileUtils;
import pt.arquivo.imagesearch.indexing.utils.ImageSearchIndexingUtil;
import pt.arquivo.imagesearch.indexing.utils.WarcPathFilter;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Hadoop process responsible for the 1nd stage of the pipeline.
 * Takes (W)ARCs, extracts image and page metadata and generates intermediate results ready for deduplication.
 * The output is a set of intermediate files ready for the 2nd stage of Hadoop processing
 */
public class DocumentIndexerWithDupsJob extends Configured implements Tool {

    /**
     * Counters for the first Hadoop process that are related to images
     * <p>
     * WARCS: number of WARCS parsed
     * WARCS_DOWNLOAD_ERROR: number of WARCS that resulted in a download error
     * <p>
     * WARCS_FAILED: number of WARCs that failed processing
     * <p>
     * WARCS_FAILED_STREAM: number of WARCs that failed during stream decoding
     * <p>
     * RECORDS_READ: number of records processed from all types (pages, images and all other)
     * RECORDS_FAILED: number of records that failed processing
     * RECORD_NEXT_FAILED: number of records that failed when progressing to the next record
     * <p>
     * IMAGES_IN_WARC_TOTAL: number of images that are present in the WARC
     * IMAGES_IN_WARC_FAILED: number of images that failed processing (decoding, getting resolution, ...)
     * <p>
     * IMAGES_IN_WARC_TOO_SMALL: images that were not processed due to being too small (below 50x50 px)
     * IMAGES_IN_WARC_TOO_SMALL_BASE64: images that were not processed due to being too small in base64
     * IMAGES_IN_WARC_TOO_LARGE: images that were not processed due to being too large (area above 15000*15000)
     * IMAGES_IN_WARC_MIME_INVALID: images where the mimetype could not be detected
     * IMAGES_IN_WARC_MIME_WRONG: images where the detected mimetipe differs from the reported mimetype
     * <p>
     * IMAGES_IN_WARC_PARSED: number of images that were effectively parsed
     * IMAGES_IN_WARC_PARSED_DUP: number of images that were parsed (with duplicates from the same WARC removed)
     */
    public enum DOCUMENT_COUNTERS {
        WARCS,
        WARCS_DOWNLOAD_ERROR,

        WARCS_FAILED,

        WARCS_FAILED_STREAM,
        TIKA_RECORDS_FAILED_DIGEST,

        RECORDS_READ,
        RECORDS_PREPARSING_FAILED,
        RECORDS_TIKA_READ,
        RECORDS_TIKA_FAILED,
        RECORDS_TIKA_FAILED_NO_SUCH_METHOD,
        RECORDS_TIKA_PARSED_MIME,
        RECORDS_TIKA_IGNORED_MIME_DETECTED,
        
        RECORDS_PARSING_FAILED,
        RECORDS_SUCCESS,
        
        REDUCE_UNIQUE_RECORDS,
        REDUCE_TOTAL_RECORDS,
        
    }

    public static class Map extends Mapper<LongWritable, Text, Text, Writable> {

        private final Logger logger = Logger.getLogger(Map.class);
        public String collection;
        DocumentInformationExtractor indexer;
        private String warcFileTempBaseDir;

        @Override
        public void setup(Context context) {
            String logLevel = System.getenv("INDEXING_LOG_LEVEL");
            if (logLevel != null) {
                org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.toLevel(logLevel));
            } else {
                org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.ERROR);
            }
            //logger.setLevel(Level.DEBUG);
            Configuration config = context.getConfiguration();
            collection = config.get("collection");
            logger.debug(collection + "_Docs/docs/");
            this.collection = config.get("collection");
            this.warcFileTempBaseDir = config.get("warcFileTempBaseDir");
            // Make dir if not exists
            File dir = new File(warcFileTempBaseDir);
            if (!dir.exists()) {
                dir.mkdirs();
            }
            indexer = new DocumentInformationExtractor(collection, context);
        }

        /**
         * First stage hadoop processing
         * <p>
         * Downloads the (W)ARC locally and processes it into all required metadata.
         * Entries are written into their corresponding SURT entries
         *
         * @param key     Hadoop key (not required at this stage)
         * @param value   (W)ARC url
         * @param context Hadoop context
         * @throws IOException unrecoverable errors processing (W)ARCs are thrown so that Hadoop retries it
         */
        public void map(LongWritable key, Text value, Context context) throws IOException {
            String arcURL = value.toString();
            if (!arcURL.isEmpty()) {
                logger.info("(W)ARCNAME: " + arcURL);
                context.getCounter(DOCUMENT_COUNTERS.WARCS).increment(1);

                URL url = null;
                try {
                    url = new URL(arcURL);
                } catch (MalformedURLException e) {
                    context.getCounter(DOCUMENT_COUNTERS.WARCS_FAILED).increment(1);
                    throw e;
                }
                String[] surl = url.getPath().split("/");
                String arcName = surl[surl.length - 1];
                String filename = warcFileTempBaseDir + "/" + System.currentTimeMillis() + "_" + arcName;

                try {
                    long fileSize = ImageSearchIndexingUtil.getFileSize(url);
                    //ImageSearchIndexingUtil.saveFile(url, filename);
                    File dest = new File(filename);

                    // download and parse WARC locally to avoid problems when streaming from remote server
                    AlternativeFileUtils.copyURLToFile(url, dest, 1000 * 60, 1000 * 30);
                    dest = new File(filename);
                    if (fileSize != dest.length()) {
                        long localFileSize = dest.length();
                        FileUtils.deleteQuietly(dest);
                        throw new IOException("Incomplete file: Local file and remote file have different sizes. Remote URL: " + url + " Remote file size: " + fileSize + " Local file name: " + filename + " Local file size: " + localFileSize);
                    }
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                    context.getCounter(DOCUMENT_COUNTERS.WARCS_DOWNLOAD_ERROR).increment(1);
                    File dest = new File(filename);
                    FileUtils.deleteQuietly(dest);
                    throw e;
                }
                File dest = new File(filename);
                logger.info("(W)ARC downloaded: " + dest.getAbsolutePath());

                indexer.parseRecord(arcName, dest.getPath());
                FileUtils.deleteQuietly(dest);

            }
        }


        /**
         * So, results are only written at Hadoop cleanup stage (after all maps are finished) so that fewer duplicates are sent to the next stage
         *
         * @param context Hadoop context which will contain all the processed metadata
         * @throws IOException          error writing to Hadoop context
         * @throws InterruptedException error calling parent super.cleanup
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            logger.info("Cleanup");
            super.cleanup(context);
            for (java.util.Map.Entry<String, TextDocumentData> entry : indexer.getEntries().entrySet()) {
                String surt = entry.getKey();
                context.write(new Text(surt), entry.getValue());
            }
        }
    }

    public static class Reduce extends Reducer<Text, Writable, NullWritable, Text> {

        private final Logger logger = Logger.getLogger(Reduce.class);
        public String collection;

        @Override
        public void setup(Reducer.Context context) {
            Configuration config = context.getConfiguration();
            collection = config.get("collection");

            this.collection = config.get("collection");
        }


        /**
         * Redude process that takes image and page records grouped by SURT and merges them by image digest
         *
         * @param key     SURT for that specific entry
         * @param values  Image and page metadatasa for that SURT
         * @param context Hadoop context
         */
        public void reduce(Text key, Iterable<Writable> values, Context context) throws IOException, InterruptedException {
            Gson gson = new GsonBuilder()
                    .registerTypeAdapter(TextDocumentData.class, new TextDocumentDataSerializer())
                    .disableHtmlEscaping()
                    .create();
            context.getCounter(DOCUMENT_COUNTERS.REDUCE_UNIQUE_RECORDS).increment(1);
            for (Writable value : values) {
                TextDocumentData metadata = (TextDocumentData) value;
                context.getCounter(DOCUMENT_COUNTERS.REDUCE_TOTAL_RECORDS).increment(1);
                try {
                    context.write(NullWritable.get(), new Text(gson.toJson(metadata)));
                } catch (IOException | InterruptedException e) {
                    logger.error("Error writing output", e);
                }

            }
        }
    }


    /**
     * Class entry point, process all (W)ARCs and output intermediary non-deduplicated results ready for the next Hadoop stage
     *
     * @param args: args[0]: HDFS file with (W)ARC file list, args[1]: collection name, args[2]: (W)ARC files per map, args[3]: number of reduces, args[4]: are (W)ARCs in HDFS (true, false), (optional) args[5]: output HDFS dir
     * @return 0 if successful, 1 otherwise
     * @throws Exception crash if there is an error getting required files from HDFS
     */
    @Override
    public int run(String[] args) throws Exception {
        String logLevel = System.getenv("INDEXING_LOG_LEVEL");
        if (logLevel != null) {
            org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.toLevel(logLevel));
        } else {
            org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.ERROR);
        }

        assert args.length >= 1 : "Missing hdfs file with all arcs path argument";
        String hdfsArcsPath = args[0];

        assert args.length >= 2 : "Missing collection name argument";
        String collection = args[1];
        String jobName = collection + "_DocumentIndexerWithDups";

        assert args.length >= 3 : "Missing number of warcs per map";
        int linesPerMap = Integer.parseInt(args[2]);

        assert args.length >= 4 : "Missing number of reduces";
        int reducesCount = Integer.parseInt(args[3]);

        assert args.length >= 5 : "Missing modeIsHDFS";
        boolean modeIsHDFS = Boolean.parseBoolean(args[4]);

        assert args.length >= 6 : "Missing output dir";
        String outputDir = args[5];

        assert args.length >= 7 : "Missing warcFileTempBaseDir";
        String warcFileTempBaseDir = args[6];

        Configuration conf = new Configuration();
        conf.set("collection", collection);
        conf.set("warcFileTempBaseDir", warcFileTempBaseDir);

        Job job = Job.getInstance(conf);

        job.setJarByClass(DocumentIndexerWithDupsJob.class);

        job.setMapperClass(DocumentIndexerWithDupsJob.Map.class);
        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.addInputPath(job, new Path(hdfsArcsPath));
        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", linesPerMap);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TextDocumentData.class);

        job.setReducerClass(DocumentIndexerWithDupsJob.Reduce.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setJobName(jobName);

        //job.getConfiguration().setInt("mapreduce.job.running.map.limit", 80);

        // sometimes, jobs can fail for recoverable reasons (errors getting WARC from document server
        // by setting the retry amount to 6, we ensure that only Maps from unusable WARCs fail processing
        job.getConfiguration().setInt("mapreduce.map.maxattempts", 6);
        job.getConfiguration().setInt("mapreduce.reduce.shuffle.parallelcopies", 10);

        // increased timeout ensure that even the most complex and largest (W)ARCS are processed
        job.getConfiguration().setInt("mapreduce.task.timeout", 5400000);

        job.setNumReduceTasks(reducesCount);

        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(new Path(outputDir)))
            hdfs.delete(new Path(outputDir), true);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new DocumentIndexerWithDupsJob(), args);
        System.exit(exitCode);
    }
}