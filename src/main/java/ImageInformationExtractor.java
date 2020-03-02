import com.sun.jersey.core.util.Base64;
import data.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.apache.log4j.Logger;
import org.archive.io.arc.ARCRecord;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import utils.WARCInformationParser;
import utils.WARCRecordResponseEncapsulated;

import java.io.*;
import java.net.URL;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class ImageInformationExtractor {

    private Logger logger = Logger.getLogger(ImageInformationExtractor.class);

    private HashMap<String, PageImageData> imgSrcEntries;
    private HashMap<String, ImageData> imgFileEntries;
    //private PageImageDataComparator comparatorPages;
    //private ImageDataComparator comparatorImages;
    private String collection;
    private Mapper<LongWritable, Text, Text, Text>.Context context;
    private HashMap<Enum<?>, Counter> localCounters;

    public ImageInformationExtractor(String collection, Mapper<LongWritable, Text, Text, Text>.Context context) {
        imgSrcEntries = new HashMap<>();
        imgFileEntries = new HashMap<>();
        //comparatorPages = new PageImageDataComparator();
        //comparatorImages = new ImageDataComparator();

        this.collection = collection;

        this.context = context;
    }

    public ImageInformationExtractor(String collection) {
        imgSrcEntries = new HashMap<>();
        imgFileEntries = new HashMap<>();
        //comparatorPages = new PageImageDataComparator();
        //comparatorImages = new ImageDataComparator();

        this.collection = collection;

        this.localCounters = new HashMap<>();
    }

    public Counter getCounter(Enum<?> counterName) {
        if (context != null) {
            return context.getCounter(counterName);
        } else {
            if (localCounters.get(counterName) == null)
                localCounters.put(counterName, new GenericCounter(counterName.name(), counterName.name()));
            return localCounters.get(counterName);
        }
    }

    public void parseRecord(String arcURL) {
        if (arcURL.endsWith("warc.gz") || arcURL.endsWith("warc")) {
            parseWarcEntryRecord(arcURL);
        } else {
            parseArcEntry(arcURL);
        }
    }

    public void parseWarcEntryRecord(String arcURL) {
        ImageSearchIndexingUtil.readWarcRecords(arcURL, this, (record) -> {

            String mimetype = record.getContentMimetype();
            if (mimetype != null) {
                if (mimetype.contains("image")) {
                    createImageDB(arcURL, record, context);
                }
                if (mimetype.contains("html")) { /*only processing images*/
                    logger.debug("Searching images in html record");
                    parseImagesFromHtmlRecord(context, record.getContentBytes(), record.getWARCRecord().getHeader().getUrl(), record.getTs());
                }
            }
        });

    }

    public void parseArcEntry(String arcURL) {
        ImageSearchIndexingUtil.readArcRecords(arcURL, this, record -> {
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

    public ImageData saveImageMetadataInline(String url, String timestamp, Mapper.Context context) {
        try {
            String[] surl = url.split(",");


            String[] metadata = surl[0].split(";");
            String reportedMimeType = metadata[0].split(":")[1];

            String data = url.substring(surl[0].length() + 1);
            String imageURLHashKey = ImageSearchIndexingUtil.md5ofString(url);

            byte[] contentBytes = data.getBytes();
            for (String meta : metadata) {
                if (meta.contains("base64")) {
                    contentBytes = Base64.decode(data);
                    break;
                }
            }

            return saveImageMetadata("hash:" + imageURLHashKey, imageURLHashKey, timestamp, reportedMimeType, contentBytes, context);
        } catch (Exception e) {
            logger.error(String.format("Malformed inline image"));
            return null;
        }
    }


    public ImageData saveImageMetadata(String url, String imageHashKey, String timestamp, String reportedMimeType, byte[] contentBytes, Mapper.Context context) {

        String imgSurt = WARCInformationParser.toSURT(url);


        String detectedMimeType = "";

        try {

            detectedMimeType = WARCInformationParser.getMimeType(contentBytes);

            if (detectedMimeType == null) {
                this.getCounter(ImageIndexerWithDups.IMAGE_COUNTERS.IMAGES_IN_WARC_MIME_INVALID).increment(1);
                detectedMimeType = "";
            } else if (!detectedMimeType.isEmpty() && !detectedMimeType.equals(reportedMimeType)) {
                logger.debug(String.format("MimeType for http://arquivo.pt/wayback/%s/%s", timestamp, url));
                logger.debug(String.format("reported: \"%s\" ; detected: \"%s\"", reportedMimeType, detectedMimeType));
                this.getCounter(ImageIndexerWithDups.IMAGE_COUNTERS.IMAGES_IN_WARC_MIME_WRONG).increment(1);
            }
        } catch (Exception e) {
            this.getCounter(ImageIndexerWithDups.IMAGE_COUNTERS.IMAGES_IN_WARC_MIME_INVALID).increment(1);
        }

        ImageData imageData = new ImageData(imageHashKey, timestamp, url, imgSurt, reportedMimeType, detectedMimeType, this.collection, contentBytes, 1);
        ImageData imageDataOld;

        try {
            imageData = ImageParse.getPropImage(imageData);
        } catch (Exception | StackOverflowError e) {
            this.getCounter(ImageIndexerWithDups.IMAGE_COUNTERS.IMAGES_IN_WARC_FAILED).increment(1);
            return null;
        }


        if (imageData == null) {
            this.getCounter(ImageIndexerWithDups.IMAGE_COUNTERS.IMAGES_IN_WARC_FAILED).increment(1);
        } else if (url.startsWith("hash:") && (imageData.getWidth() < ImageParse.MIN_WIDTH || imageData.getHeight() < ImageParse.MIN_HEIGHT)) {
            this.getCounter(ImageIndexerWithDups.IMAGE_COUNTERS.IMAGES_IN_WARC_TOO_SMALL_BASE64).increment(1);
        } else if (imageData.getWidth() < ImageParse.MIN_WIDTH || imageData.getHeight() < ImageParse.MIN_HEIGHT) {
            this.getCounter(ImageIndexerWithDups.IMAGE_COUNTERS.IMAGES_IN_WARC_TOO_SMALL).increment(1);
        } else if (imageData.getWidth() * imageData.getHeight() > ImageParse.MAX_HEIGHT * ImageParse.MAX_HEIGHT) {
            this.getCounter(ImageIndexerWithDups.IMAGE_COUNTERS.IMAGES_IN_WARC_TOO_LARGE).increment(1);
        } else {

            this.getCounter(ImageIndexerWithDups.IMAGE_COUNTERS.IMAGES_IN_WARC_PARSED_DUP).increment(1);
            if ((imageDataOld = imgFileEntries.get(imageData.getSurt())) != null) {
                imageDataOld.addTimestamps(imageData.getTimestamp());
            } else {
                this.getCounter(ImageIndexerWithDups.IMAGE_COUNTERS.IMAGES_IN_WARC_PARSED).increment(1);
                imgFileEntries.put(imageData.getSurt(), imageData);
            }
            return imageData;
                /*Gson gson = new Gson();
                try {
                    context.write(new Text(imgSurt), new Text(gson.toJson(imageData)));
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                */
        }
        return null;
    }

    public void createImageDB(String arcURL, WARCRecordResponseEncapsulated record, Mapper.Context context) {
        String url = "";
        String timestamp = "";
        try {
            url = record.getWARCRecord().getHeader().getUrl();
            timestamp = record.getTs();
            String mime = record.getContentMimetype();

            String imageURLHashKey = ImageSearchIndexingUtil.md5ofString(url);
            byte[] contentBytes = null;

            this.getCounter(ImageIndexerWithDups.IMAGE_COUNTERS.IMAGES_IN_WARC_TOTAL).increment(1);

            try {
                contentBytes = record.getContentBytes();
            } catch (RuntimeException e) {
                logger.error(String.format("Error getting record content bytes for image url: %s/%s with error message %s", timestamp, url, e.getMessage()));
                this.getCounter(ImageIndexerWithDups.IMAGE_COUNTERS.IMAGES_IN_WARC_FAILED).increment(1);
                return;
            }

            saveImageMetadata(url, imageURLHashKey, timestamp, mime, contentBytes, context);

        } catch (Exception e) {
            logger.error(String.format("Error parsing image url: %s/%s with error message %s", timestamp, url, e.getMessage()));
            this.getCounter(ImageIndexerWithDups.IMAGE_COUNTERS.IMAGES_IN_WARC_FAILED).increment(1);
            return;
        }
    }

    public ImageData createImageDB(String arcURL, ARCRecord record, Mapper.Context context) {
        String url = record.getHeader().getUrl();
        String timestamp = record.getMetaData().getDate();
        String mime = record.getMetaData().getMimetype();
        String imageURLHashKey = ImageSearchIndexingUtil.md5ofString(url);

        byte[] contentBytes;

        this.getCounter(ImageIndexerWithDups.IMAGE_COUNTERS.IMAGES_IN_WARC_TOTAL).increment(1);

        try {
            contentBytes = ImageSearchIndexingUtil.getRecordContentBytes(record);
        } catch (IOException e) {
            logger.error(String.format("Error getting record content bytes for image url: %s/%s on offset %d with error message %s", timestamp, url, record.getBodyOffset(), e.getMessage()));
            this.getCounter(ImageIndexerWithDups.IMAGE_COUNTERS.IMAGES_IN_WARC_FAILED).increment(1);
            return null;
        }

        return saveImageMetadata(url, imageURLHashKey, timestamp, mime, contentBytes, context);
    }

    public void parseImagesFromHtmlRecord(Mapper.Context context, byte[] arcRecordBytes, String pageURL, String
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

            this.getCounter(ImageIndexerWithDups.PAGE_COUNTERS.IMAGES_IN_HTML_TOTAL).increment(pageImages);

            this.getCounter(ImageIndexerWithDups.PAGE_COUNTERS.PAGES).increment(1);

            if (imgs.size() == 0)
                return;

            this.getCounter(ImageIndexerWithDups.PAGE_COUNTERS.PAGES_WITH_IMAGES).increment(1);


            String pageURLCleaned = URLDecoder.decode(pageURL, "UTF-8"); /*Escape URL e.g %C3*/
            //pageURLCleaned = StringUtils.stripAccents(pageURLCleaned); /* Remove accents*/
            String pageURLTokens = ImageSearchIndexingUtil.parseURL(pageURLCleaned); /*split the URL*/


            URL uri = new URL(pageURL);
            String pageHost = uri.getHost();
            String pageProtocol = uri.getProtocol();

            if (pageTstamp == null || pageTstamp.equals("")) {
                logger.debug("Null pageTstamp");
                pageTstamp = "";
            }
            logger.debug("pageTstamp:" + pageTstamp);

            Set<String> imgSrcParsed = new HashSet<>();

            for (Element el : imgs) {
                String imgSrc = el.attr("abs:src");
                String imgRelSrc = el.attr("src").trim();

                boolean alreadyFoundInPage = imgSrcParsed.contains(imgRelSrc);
                imgSrcParsed.add(imgRelSrc);


                logger.debug("Getting information for: " + imgSrc);
                if (imgRelSrc.startsWith("data:image")) {
                    logger.debug("Inline image");
                    ImageData acceptedRecord = saveImageMetadataInline(imgRelSrc, pageTstamp, context);
                    this.getCounter(ImageIndexerWithDups.PAGE_COUNTERS.IMAGES_IN_HTML_BASE64).increment(1);
                    if (acceptedRecord == null)
                        continue;
                    imgSrc = acceptedRecord.getUrl();
                } else if (imgSrc.length() > 10000 || pageURL.length() > 10000) {
                    logger.debug("URL of image too big ");
                    logger.debug(pageURL.substring(0, 500) + "...");
                    this.getCounter(ImageIndexerWithDups.PAGE_COUNTERS.IMAGES_IN_HTML_FAILED).increment(1);
                    continue;
                } else if (imgRelSrc == null || imgRelSrc.equals("")) {
                    logger.debug("Null imgSrc");
                    this.getCounter(ImageIndexerWithDups.PAGE_COUNTERS.IMAGES_IN_HTML_INVALID).increment(1);
                    continue;
                }

                this.getCounter(ImageIndexerWithDups.PAGE_COUNTERS.IMAGES_IN_HTML_MATCHING).increment(1);

                String imgSrcCleaned = URLDecoder.decode(imgSrc, "UTF-8"); /*Escape imgSrc URL e.g %C3*/
                //imgSrcCleaned = StringUtils.stripAccents(imgSrcCleaned); /* Remove accents*/
                String imgSrcTokens = ImageSearchIndexingUtil.parseURL(imgSrcCleaned); /*split the imgSrc URL*/

                String imgTitle = el.attr("title");
                if (imgTitle.length() > 9999) {
                    imgTitle = imgTitle.substring(0, 10000);
                }
                String imgAlt = el.attr("alt");
                if (imgAlt.length() > 9999) {
                    imgAlt = imgAlt.substring(0, 10000);
                }


                insertImageIndexes(imgSrc, imgSrcTokens, imgTitle, imgAlt, pageImages, pageTstamp, pageURL, pageHost, pageProtocol, pageTitle, pageURLTokens, alreadyFoundInPage, context);

                logger.debug("Written to file - successfully indexed image record");

            }
        } catch (Exception e) {
            logger.debug("Something failed JSOUP parsing " + e.getMessage());
        }

    }

    private void insertImageIndexes(String imgSrc, String imgSrcTokens, String imgTitle, String imgAlt,
                                    int pageImages, String pageTstamp, String pageURL, String pageHost, String pageProtocol, String
                                            pageTitle, String pageURLTokens, boolean alreadyFoundInPage, Mapper<LongWritable, Text, Text, Text>.Context context) {
        String imgSurtSrc = WARCInformationParser.toSURT(imgSrc);

        PageImageData pageImageData = new PageImageData("page", imgTitle, imgAlt, imgSrcTokens, pageTitle, pageURLTokens, imgSrc, imgSurtSrc, pageImages, pageImages, 1, pageTstamp, pageURL, pageHost, pageProtocol);
        PageImageData pageImageDataOld = null;

        if (!alreadyFoundInPage)
            pageImageData.incrementMatchingPages(1);

        this.getCounter(ImageIndexerWithDups.PAGE_COUNTERS.IMAGES_IN_HTML_SENT_DUP).increment(1);
        if ((pageImageDataOld = imgSrcEntries.get(pageImageData.getImageSurt())) == null) {
            this.getCounter(ImageIndexerWithDups.PAGE_COUNTERS.IMAGES_IN_HTML_SENT).increment(1);
            imgSrcEntries.put(pageImageData.getImageSurt(), pageImageData);
        } else {
            boolean imageMetadataChanged = pageImageDataOld.addPageImageData(pageImageData);
            if (imageMetadataChanged) {
                this.getCounter(ImageIndexerWithDups.PAGE_COUNTERS.IMAGES_IN_HTML_METADATA_CHANGED).increment(1);
            }
        }
    }

    public HashMap<String, PageImageData> getImgSrcEntries() {
        return imgSrcEntries;
    }

    public HashMap<String, ImageData> getImgFileEntries() {
        return imgFileEntries;
    }
}
