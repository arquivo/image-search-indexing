import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import data.ImageData;
import data.PageImageData;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.kerby.util.Base64;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.archive.io.arc.ARCRecord;

import org.archive.url.SURT;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLDecoder;


public class FullImageIndexer {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private Logger logger = Logger.getLogger(ImageMap.class);
        public String collection;

        @Override
        public void setup(Context context) {
            logger.setLevel(Level.DEBUG);
            Configuration config = context.getConfiguration();
            collection = config.get("collection");
            System.out.println("collection: " + collection);


            logger.debug(collection + "_Images" + "/img/");
            this.collection = config.get("collection");
            System.out.println("collection: " + this.collection);

        }

        @Override
        protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {

            super.cleanup(context);
        }

        public void saveImageMetadata(String url, String image_hash_key, String tstamp, String mime, String content_hash, byte[] contentBytes, Context context) {
            String imgSurtSrc = SURT.toSURT(url);


            ImageData imageData = new ImageData(image_hash_key, tstamp, url, imgSurtSrc, mime, this.collection, content_hash, Base64.encodeBase64String(contentBytes));
            Gson gson = new Gson();


            try {
                context.write(new Text(imgSurtSrc), new Text(gson.toJson(imageData)));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        public void createImageDB(String arcURL, WARCRecordResponseEncapsulated record, Context context) {
            String url = record.getWARCRecord().getHeader().getUrl();
            String tstamp = record.getTs();
            String mime = record.getContentMimetype();

            String image_hash_key = ImageSearchIndexingUtil.md5ofString(url);
            String content_hash = ImageSearchIndexingUtil.md5ofString(tstamp + "/" + url);
            byte[] contentBytes = null;
            contentBytes = record.getContentBytes();

            saveImageMetadata(url, image_hash_key, tstamp, mime, content_hash, contentBytes, context);


        }

        public void createImageDB(String arcURL, ARCRecord record, Context context) {
            String url = record.getHeader().getUrl();
            String tstamp = record.getMetaData().getDate();
            String mime = record.getMetaData().getMimetype();
            String image_hash_key = ImageSearchIndexingUtil.md5ofString(url);
            String content_hash = ImageSearchIndexingUtil.md5ofString(tstamp + "/" + url);
            byte[] contentBytes;
            try {
                contentBytes = ImageSearchIndexingUtil.getRecordContentBytes(record);
            } catch (IOException e) {
                logger.error(String.format("Error getting record content bytes for image url: %s on offset %d with error message %s", url, record.getBodyOffset(), e.getMessage()));
                return;
            }

            saveImageMetadata(url, image_hash_key, tstamp, mime, content_hash, contentBytes, context);
        }

        public void parseImagesFromHtmlRecord(Context context, byte[] arcRecordBytes, String pageURL, String pageTstamp) {
            try {
                logger.debug("Parsing Images from (W)ARCrecord");
                logger.debug("Number of content bytes: " + arcRecordBytes.length);
                logger.debug("URL: " + pageURL);
                logger.debug("Page TS: " + pageTstamp);

                logger.info("Parsing Images from (W)ARCrecord");
                logger.info("Read Content Bytes from (W)ARCrecord");
                String recordEncoding = ImageSearchIndexingUtil.guessEncoding(arcRecordBytes);
                InputStream is = new ByteArrayInputStream(arcRecordBytes);

                Document doc = Jsoup.parse(is, recordEncoding, "");

                doc.setBaseUri(pageURL);

                String pageTitle = doc.title(); /*returns empty string if no title in html document*/
                Elements imgs = doc.getElementsByTag("img");
                int pageImages = imgs.size();

                logger.debug("Page contains: " + pageImages + " images");

                String pageURLCleaned = URLDecoder.decode(pageURL, "UTF-8"); /*Escape URL e.g %C3*/
                pageURLCleaned = StringUtils.stripAccents(pageURLCleaned); /* Remove accents*/
                String pageURLTokens = ImageSearchIndexingUtil.parseURL(pageURLCleaned); /*split the URL*/


                URL uri = new URL(pageURL);
                String pageHost = uri.getHost();
                String pageProtocol = uri.getProtocol();
                //String pageTstamp = record.getMetaData().getDate();
                if (pageTstamp == null || pageTstamp.equals("")) {
                    logger.debug("Null pageTstamp");
                }
                logger.debug("pageTstamp:" + pageTstamp);

                long startTime, timeElapsed = System.currentTimeMillis();
                for (Element el : imgs) {
                    String imgSrc = el.attr("abs:src");

                    logger.debug("Getting information for: " + imgSrc);
                    if (imgSrc.length() > 10000 || pageURL.length() > 10000) {
                        logger.debug("URL of image too big ");
                        logger.debug(pageURL.substring(0, 500) + "...");
                        continue;
                    }/*Maximum size for SOLR index is 10 000*/
                    if (imgSrc == null || imgSrc.equals("")) {
                        logger.debug("Null imgSrc");
                        continue;
                    } else {

                        String imgSrcCleaned = URLDecoder.decode(imgSrc, "UTF-8"); /*Escape imgSrc URL e.g %C3*/
                        imgSrcCleaned = StringUtils.stripAccents(imgSrcCleaned); /* Remove accents*/
                        String imgSrcTokens = ImageSearchIndexingUtil.parseURL(imgSrcCleaned); /*split the imgSrc URL*/

                        String imgTitle = el.attr("title");
                        if (imgTitle.length() > 9999) {
                            imgTitle = imgTitle.substring(0, 10000);
                        }
                        String imgAlt = el.attr("alt");
                        if (imgAlt.length() > 9999) {
                            imgAlt = imgAlt.substring(0, 10000);
                        }
                        insertImageIndexes(imgSrc, imgSrcTokens, imgTitle, imgAlt, pageImages, pageTstamp, pageURL, pageHost, pageProtocol, pageTitle, pageURLTokens, context);

                        logger.debug("Written to file - successfully indexed image record");

                    }
                }
            } catch (Exception e) {
                logger.debug("Something failed JSOUP parsing " + e.getMessage());
            }

        }

        private void insertImageIndexes(String imgSrc, String imgSrcTokens, String imgTitle, String imgAlt, int pageImages, String pageTstamp, String pageURL, String pageHost, String pageProtocol, String pageTitle, String pageURLTokens, Mapper<LongWritable, Text, Text, Text>.Context context) {
            try {
                String imgSurtSrc = SURT.toSURT(imgSrc);

                PageImageData pageImageData = new PageImageData(imgTitle, imgAlt, imgSrcTokens, pageTitle, pageURLTokens, imgSrc, imgSurtSrc, pageImages, pageTstamp, pageURL, pageHost, pageProtocol);
                Gson gson = new Gson();
                context.write(new Text(imgSurtSrc), new Text(gson.toJson(pageImageData)));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }


        }


        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String arcURL = value.toString();

            System.out.println("(W)ARCNAME: " + arcURL);
            logger.info("(W)ARCNAME: " + arcURL);

            if (arcURL.endsWith("warc.gz") || arcURL.endsWith("warc")) {
                ImageSearchIndexingUtil.readWarcRecords(arcURL, (record) -> {
                    boolean isImage = record.getContentMimetype().contains("image");
                    if (isImage) {
                        createImageDB(arcURL, record, context);
                    }
                    if (record != null && record.getContentMimetype() != null && record.getContentMimetype().contains("html")) { /*only processing images*/
                        logger.debug("Searching images in html record");
                        parseImagesFromHtmlRecord(context, record.getContentBytes(), record.getWARCRecord().getHeader().getUrl(), record.getTs());
                    }
                });
            } else {
                ImageSearchIndexingUtil.readArcRecords(arcURL, record -> {
                    boolean isImage = record.getMetaData().getMimetype().contains("image");
                    if (isImage) {
                        createImageDB(arcURL, record, context);
                    }
                    if (record.getMetaData().getMimetype().contains("html")) {
                        byte[] recordContentBytes;
                        try {
                            recordContentBytes = ImageSearchIndexingUtil.getRecordContentBytes(record);
                        } catch (IOException e) {
                            logger.error(String.format("Error getting record content bytes for (w)arc: %s on offset %d with error message %s", arcURL, record.getBodyOffset(), e.getMessage()));
                            return;
                        }
                        parseImagesFromHtmlRecord(context, recordContentBytes, record.getHeader().getUrl(), record.getMetaData().getDate());
                    }
                });
            }

        }


    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) {
            Gson gson = new Gson();
            int i = 0;
            for (Text val : values) {
                try {
                    PageImageData page = gson.fromJson(val.toString(), PageImageData.class);
                    //WIP: do stuff
                } catch (JsonSyntaxException e){
                    ImageData page = gson.fromJson(val.toString(), ImageData.class);
                    //WIP: do stuff
                }

                i++;
            }

            try {
                context.write(key, new Text("" + i));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    }

    public static void main(String[] args) throws Exception {
        assert args.length >= 1 : "Missing hdfs file with all arcs path argument";
        String hdfsArcsPath = args[0];

        assert args.length >= 2 : "Missing collection name argument";
        String collection = args[1];
        String jobName = collection + "_IndexImages_2";

        assert args.length >= 3 : "Missing argument max running map in parallel";
        int maxMaps = Integer.parseInt(args[2]);

        assert args.length >= 4 : "Missing argument max arcs per map";
        int linespermap = Integer.parseInt(args[3]);


        Configuration conf = new Configuration();
        conf.set("collection", collection);

        Job job = Job.getInstance(conf, "Index Images");
        job.setJarByClass(FullImageIndexer.class);

        job.setInputFormatClass(NLineInputFormat.class);
        job.setMapperClass(FullImageIndexer.Map.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(FullImageIndexer.Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        job.setJobName(jobName);


        NLineInputFormat.addInputPath(job, new Path(hdfsArcsPath));

        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", linespermap);
        job.getConfiguration().setInt("mapreduce.job.running.map.limit", maxMaps); /*Maximum simultaneous maps running*/

        // Sets reducer tasks to 1
        job.setNumReduceTasks(1);

        FileOutputFormat.setOutputPath(job, new Path("/user/amourao/output/" + collection));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}