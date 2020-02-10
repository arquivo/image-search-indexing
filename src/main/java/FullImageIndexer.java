import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import com.j256.simplemagic.ContentInfo;
import com.j256.simplemagic.ContentInfoUtil;
import data.FullImageMetadata;
import data.ImageData;
import data.PageImageData;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.archive.io.arc.ARCRecord;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import utils.WARCInformationParser;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLDecoder;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;

public class FullImageIndexer {

    public enum IMAGE_COUNTERS {
        WARCS,
        IMAGES_IN_WARC_TOTAL,
        IMAGES_IN_WARC_FAILED,
        IMAGES_IN_WARC_PARSED,
        IMAGES_IN_WARC_TOO_SMALL,
        IMAGES_IN_WARC_MIME_INVALID,
        IMAGES_IN_WARC_MIME_WRONG,
    }

    public enum PAGE_COUNTERS {
        IMAGES_IN_HTML_TOTAL,
        IMAGES_IN_HTML_FAILED,
        IMAGES_IN_HTML_INVALID,
        IMAGES_IN_HTML_MATCHING,
        IMAGES_IN_HTML_BASE64,
        PAGES,
        PAGES_WITH_IMAGES

    }

    public enum REDUCE_COUNTERS {
        URL_IMAGES_PAGES,
        URL_IMAGES_PAGESALL,
        URL_IMAGESALL_PAGES,
        URL_IMAGES_NPAGES,
        URL_IMAGESALL_NPAGES,
        URL_NIMAGES_PAGES,
        URL_NIMAGES_PAGES_ALL,
        IMAGES_PAGES_EXCEEDED
    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private Logger logger = Logger.getLogger(ImageMap.class);
        public String collection;

        @Override
        public void setup(Context context) {
            //logger.setLevel(Level.DEBUG);
            Configuration config = context.getConfiguration();
            collection = config.get("collection");

            logger.debug(collection + "_Images/img/");
            this.collection = config.get("collection");

        }

        public void saveImageMetadata(String url, String imageHashKey, String timestamp, String reportedMimeType, byte[] contentBytes, Context context) {

            String imgSurt = WARCInformationParser.toSURT(url);

            String detectedMimeType = "";

            try {
                ContentInfoUtil util = new ContentInfoUtil();
                ContentInfo info = util.findMatch(contentBytes);

                if (info == null) {
                    context.getCounter(IMAGE_COUNTERS.IMAGES_IN_WARC_MIME_INVALID).increment(1);
                } else {
                    detectedMimeType = info.getMimeType();
                }

                if (!detectedMimeType.isEmpty() && !detectedMimeType.equals(reportedMimeType)) {
                    logger.debug(String.format("MimeType for http://arquivo.pt/wayback/%s/%s", timestamp, url));
                    logger.debug(String.format("reported: \"%s\" ; detected: \"%s\"", reportedMimeType, detectedMimeType));
                    context.getCounter(IMAGE_COUNTERS.IMAGES_IN_WARC_MIME_WRONG).increment(1);
                }
            } catch (Exception e) {
                context.getCounter(IMAGE_COUNTERS.IMAGES_IN_WARC_MIME_INVALID).increment(1);
            }

            ImageData img = new ImageData(imageHashKey, timestamp, url, imgSurt, reportedMimeType, detectedMimeType, this.collection, contentBytes);

            try {
                img = ImageParse.getPropImage(img);
            } catch (Exception e) {
                context.getCounter(IMAGE_COUNTERS.IMAGES_IN_WARC_FAILED).increment(1);
                return;
            }

            if (img == null) {
                context.getCounter(IMAGE_COUNTERS.IMAGES_IN_WARC_FAILED).increment(1);
            } else if (img.getWidth() < ImageParse.MIN_WIDTH || img.getHeight() < ImageParse.MIN_HEIGHT) {
                context.getCounter(IMAGE_COUNTERS.IMAGES_IN_WARC_TOO_SMALL).increment(1);
            } else {
                context.getCounter(IMAGE_COUNTERS.IMAGES_IN_WARC_PARSED).increment(1);
                Gson gson = new Gson();
                try {
                    context.write(new Text(imgSurt), new Text(gson.toJson(img)));
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        public void createImageDB(String arcURL, WARCRecordResponseEncapsulated record, Context context) {
            try {
                String url = record.getWARCRecord().getHeader().getUrl();
                String timestamp = record.getTs();
                String mime = record.getContentMimetype();

                String imageURLHashKey = ImageSearchIndexingUtil.md5ofString(url);
                byte[] contentBytes = null;

                context.getCounter(IMAGE_COUNTERS.IMAGES_IN_WARC_TOTAL).increment(1);

                try {
                    contentBytes = record.getContentBytes();
                } catch (RuntimeException e) {
                    logger.error(String.format("Error getting record content bytes for image url: %s with error message %s", url, e.getMessage()));
                    context.getCounter(IMAGE_COUNTERS.IMAGES_IN_WARC_FAILED).increment(1);
                    return;
                }

                saveImageMetadata(url, imageURLHashKey, timestamp, mime, contentBytes, context);

            } catch (Exception e) {
                logger.error(String.format("Error parsing image url: %s with error message %s", arcURL, e.getMessage()));
                context.getCounter(IMAGE_COUNTERS.IMAGES_IN_WARC_FAILED).increment(1);
                return;
            }
        }

        public void createImageDB(String arcURL, ARCRecord record, Context context) {
            String url = record.getHeader().getUrl();
            String timestamp = record.getMetaData().getDate();
            String mime = record.getMetaData().getMimetype();
            String imageURLHashKey = ImageSearchIndexingUtil.md5ofString(url);

            byte[] contentBytes;

            context.getCounter(IMAGE_COUNTERS.IMAGES_IN_WARC_TOTAL).increment(1);

            try {
                contentBytes = ImageSearchIndexingUtil.getRecordContentBytes(record);
            } catch (IOException e) {
                logger.error(String.format("Error getting record content bytes for image url: %s on offset %d with error message %s", url, record.getBodyOffset(), e.getMessage()));
                context.getCounter(IMAGE_COUNTERS.IMAGES_IN_WARC_FAILED).increment(1);
                return;
            }

            saveImageMetadata(url, imageURLHashKey, timestamp, mime, contentBytes, context);
        }

        public void parseImagesFromHtmlRecord(Context context, byte[] arcRecordBytes, String pageURL, String
                pageTstamp) {
            try {
                logger.debug("Parsing Images from HTML in (W)ARCrecord");
                logger.debug("Read Content Bytes from (W)ARCrecord" + arcRecordBytes.length);
                logger.debug("URL: " + pageURL);
                logger.debug("Page TS: " + pageTstamp);


                String recordEncoding = ImageSearchIndexingUtil.guessEncoding(arcRecordBytes);
                InputStream is = new ByteArrayInputStream(arcRecordBytes);

                Document doc = Jsoup.parse(is, recordEncoding, "");

                doc.setBaseUri(pageURL);

                String pageTitle = doc.title(); /*returns empty string if no title in html document*/
                Elements imgs = doc.getElementsByTag("img");
                int pageImages = imgs.size();

                logger.debug("Page contains: " + pageImages + " images");

                context.getCounter(PAGE_COUNTERS.IMAGES_IN_HTML_TOTAL).increment(pageImages);

                context.getCounter(PAGE_COUNTERS.PAGES).increment(1);

                if (imgs.size() == 0)
                    return;

                context.getCounter(PAGE_COUNTERS.PAGES_WITH_IMAGES).increment(1);

                String pageURLCleaned = URLDecoder.decode(pageURL, "UTF-8"); /*Escape URL e.g %C3*/
                pageURLCleaned = StringUtils.stripAccents(pageURLCleaned); /* Remove accents*/
                String pageURLTokens = ImageSearchIndexingUtil.parseURL(pageURLCleaned); /*split the URL*/


                URL uri = new URL(pageURL);
                String pageHost = uri.getHost();
                String pageProtocol = uri.getProtocol();

                if (pageTstamp == null || pageTstamp.equals("")) {
                    logger.debug("Null pageTstamp");
                    pageTstamp = "";
                }
                logger.debug("pageTstamp:" + pageTstamp);

                for (Element el : imgs) {
                    String imgSrc = el.attr("abs:src");
                    String imgRelSrc = el.attr("src");

                    logger.debug("Getting information for: " + imgSrc);
                    if (imgRelSrc.startsWith("data:image")) {
                        logger.debug("Base64 image");
                        context.getCounter(PAGE_COUNTERS.IMAGES_IN_HTML_BASE64).increment(1);
                        continue;
                    }
                    if (imgSrc.length() > 10000 || pageURL.length() > 10000) {
                        logger.debug("URL of image too big ");
                        logger.debug(pageURL.substring(0, 500) + "...");
                        context.getCounter(PAGE_COUNTERS.IMAGES_IN_HTML_FAILED).increment(1);
                        continue;
                    }/*Maximum size for SOLR index is 10 000*/
                    if (imgSrc == null || imgSrc.equals("")) {
                        logger.debug("Null imgSrc");
                        context.getCounter(PAGE_COUNTERS.IMAGES_IN_HTML_INVALID).increment(1);
                        continue;
                    }

                    context.getCounter(PAGE_COUNTERS.IMAGES_IN_HTML_MATCHING).increment(1);

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
            } catch (Exception e) {
                logger.debug("Something failed JSOUP parsing " + e.getMessage());
            }

        }

        private void insertImageIndexes(String imgSrc, String imgSrcTokens, String imgTitle, String imgAlt,
                                        int pageImages, String pageTstamp, String pageURL, String pageHost, String pageProtocol, String
                                                pageTitle, String pageURLTokens, Mapper<LongWritable, Text, Text, Text>.Context context) {
            try {
                String imgSurtSrc = WARCInformationParser.toSURT(imgSrc);

                PageImageData pageImageData = new PageImageData("page", imgTitle, imgAlt, imgSrcTokens, pageTitle, pageURLTokens, imgSrc, imgSurtSrc, pageImages, pageTstamp, pageURL, pageHost, pageProtocol);
                Gson gson = new Gson();
                context.write(new Text(imgSurtSrc), new Text(gson.toJson(pageImageData)));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }


        }


        public void map(LongWritable key, Text value, Context context) {
            String arcURL = value.toString();

            logger.info("(W)ARCNAME: " + arcURL);
            context.getCounter(IMAGE_COUNTERS.WARCS).increment(1);
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
                        logger.debug("Searching images in html record");
                        parseImagesFromHtmlRecord(context, recordContentBytes, record.getHeader().getUrl(), record.getMetaData().getDate());
                    }
                });
            }

        }


    }

    public static class Reduce extends Reducer<Text, Text, NullWritable, Text> {

        private Logger logger = Logger.getLogger(ImageMap.class);
        public String collection;

        @Override
        public void setup(Reducer.Context context) {
            //logger.setLevel(Level.DEBUG);
            Configuration config = context.getConfiguration();
            collection = config.get("collection");

            logger.debug(collection + "_Images" + "/img/");
            this.collection = config.get("collection");

        }


        public void reduce(Text key, Iterable<Text> values, Context context) {
            Gson gson = new Gson();
            int pagesCount = 0;
            int imagesCount = 0;
            List<PageImageData> pages = new LinkedList<>();
            List<ImageData> images = new LinkedList<>();

            //TODO: check http://codingjunkie.net/secondary-sort  to see if it helps not having to iterate all records
            logger.debug("Reducing: " + key);
            for (Text val : values) {
                try {
                    PageImageData page = gson.fromJson(val.toString(), PageImageData.class);
                    if (page.getType() == null || !page.getType().equals("page"))
                        throw new JsonSyntaxException("");
                    pages.add(page);
                    pagesCount++;

                } catch (JsonSyntaxException e) {
                    ImageData image = gson.fromJson(val.toString(), ImageData.class);
                    images.add(image);
                    imagesCount++;
                }
                if ((pagesCount + imagesCount) > 0 && (pagesCount + imagesCount) % 100 == 0) {
                    logger.info(String.format("Still iterating: %d pages and %d images", pagesCount, imagesCount));
                }

                if ((pagesCount + imagesCount) >= 1000) {
                    logger.info(String.format("Broke iterating: %d pages and %d images", pagesCount, imagesCount));
                    context.getCounter(REDUCE_COUNTERS.IMAGES_PAGES_EXCEEDED).increment(1);
                    break;
                }

            }
            logger.debug(String.format("Found %d pages and %d images", pagesCount, imagesCount));
            if (images.size() != 0 && pages.size() != 0) {

                context.getCounter(REDUCE_COUNTERS.URL_IMAGES_PAGESALL).increment(pages.size());
                context.getCounter(REDUCE_COUNTERS.URL_IMAGESALL_PAGES).increment(images.size());
                context.getCounter(REDUCE_COUNTERS.URL_IMAGES_PAGES).increment(1);

                //logger.debug(String.format("%s: Found %d images and %d pages; image TS: \"%s\" page TS: \"%s\"", key, images.size(), pages.size(), images.get(0) == null ? "none" : images.get(0).getTimestamp().toString(), pages.get(0) == null ? "none" : pages.get(0).getTimestamp().toString()));
                ImageData image = images.get(0);

                LocalDateTime timekey = image.getTimestamp();

                PageImageData closestPage = WARCInformationParser.getClosest(pages, timekey);

                FullImageMetadata allMetadata = new FullImageMetadata(image, closestPage);

                try {
                    context.write(NullWritable.get(), new Text(gson.toJson(allMetadata)));
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else if (images.size() != 0) {
                context.getCounter(REDUCE_COUNTERS.URL_IMAGES_NPAGES).increment(1);
                context.getCounter(REDUCE_COUNTERS.URL_IMAGESALL_NPAGES).increment(images.size());
            } else if (pages.size() != 0) {
                context.getCounter(REDUCE_COUNTERS.URL_NIMAGES_PAGES).increment(1);
                context.getCounter(REDUCE_COUNTERS.URL_NIMAGES_PAGES_ALL).increment(pages.size());
            }

        }


    }

    public static void main(String[] args) throws Exception {
        assert args.length >= 1 : "Missing hdfs file with all arcs path argument";
        String hdfsArcsPath = args[0];

        assert args.length >= 2 : "Missing collection name argument";
        String collection = args[1];
        String jobName = collection + "_FullIndexer";


        Configuration conf = new Configuration();
        conf.set("collection", collection);

        Job job = Job.getInstance(conf);
        job.setJarByClass(FullImageIndexer.class);
        job.setInputFormatClass(NLineInputFormat.class);

        job.setMapperClass(FullImageIndexer.Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(FullImageIndexer.Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setJobName(jobName);

        NLineInputFormat.addInputPath(job, new Path(hdfsArcsPath));


        //job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", linespermap);
        //job.getConfiguration().setInt("mapreduce.job.running.map.limit", maxMaps); /*Maximum simultaneous maps running*/
        // Sets reducer tasks to 1
        //TODO: fix setting this number here
        job.setNumReduceTasks((int) (112 * 1.25 * 2));
        //job.setNumReduceTasks(1);

        //job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", linespermap);
        //job.getConfiguration().setInt("mapreduce.job.running.map.limit", maxMaps); /*Maximum simultaneous maps running*/

        String outputDir = "/user/amourao/output/" + collection;
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(new Path(outputDir)))
            hdfs.delete(new Path(outputDir), true);

        boolean result = job.waitForCompletion(true);

        System.out.println("FullImageIndexer$IMAGE_COUNTERS");
        Counters cn = job.getCounters();
        CounterGroup counterGroup = cn.getGroup("FullImageIndexer$IMAGE_COUNTERS");
        for (Counter c : counterGroup) {
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        System.out.println("FullImageIndexer$PAGE_COUNTERS");
        counterGroup = cn.getGroup("FullImageIndexer$PAGE_COUNTERS");
        for (Counter c : counterGroup) {
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        System.out.println("FullImageIndexer$REDUCE_COUNTERS");
        counterGroup = cn.getGroup("FullImageIndexer$REDUCE_COUNTERS");
        for (Counter c : counterGroup) {
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        System.exit(result ? 0 : 1);
    }
}