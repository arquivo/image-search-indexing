package pt.arquivo.imagesearch.indexing.processors;

import com.sun.jersey.core.util.Base64;
import org.apache.commons.io.IOUtils;
import pt.arquivo.imagesearch.indexing.ImageIndexerWithDupsJob;
import pt.arquivo.imagesearch.indexing.utils.ImageParse;
import pt.arquivo.imagesearch.indexing.data.*;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.apache.log4j.Logger;
import org.archive.io.arc.ARCRecord;
import org.jsoup.Jsoup;
import org.jsoup.helper.StringUtil;
import org.jsoup.nodes.Attribute;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import pt.arquivo.imagesearch.indexing.utils.ImageSearchIndexingUtil;
import pt.arquivo.imagesearch.indexing.utils.WARCInformationParser;
import pt.arquivo.imagesearch.indexing.utils.WARCRecordResponseEncapsulated;

import javax.imageio.ImageIO;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ImageInformationExtractor {

    //private static final boolean IGNORE_IMAGE_FILE_EXTENSIONS = true;
    private static final Set<String> IMAGE_FILE_EXTENSIONS = new HashSet<>(Arrays.asList("jpg", "jpeg", "png", "tif", "tiff", "gif", "svg", "webp", "bmp", "ico"));

    private static final Set<String> IMAGE_TAG_ATTRIBUTES_WITH_FILES = new HashSet<>(Arrays.asList("src", "lowsrc"));
    private static final Set<String> IMAGE_TAG_JS_ATTRIBUTES_WITH_FILES = new HashSet<>(Arrays.asList("onLoad"));

    private static final Pattern CSS_URLS = Pattern.compile("url\\(['\"]*(.*?)['\"]*\\)");

    public static final int MAX_PARENT_CAPTION_SIZE = 250;
    public static final int MAX_IMAGE_FIELD_SIZE = 10000;
    public static final String DATA_IMAGE_URL_PREFIX = "data:image";
    public static final int MAX_IMAGE_IN_HTML = 10000;


    private Logger logger = Logger.getLogger(ImageInformationExtractor.class);

    //private HashMap<String, PageImageData> imgSrcEntries;
    //private HashMap<String, ImageData> imgFileEntries;
    private HashMap<String, FullImageMetadata> entries;
    private String collection;
    private Mapper.Context context;
    private HashMap<Enum<?>, Counter> localCounters;

    public ImageInformationExtractor(String collection, Mapper.Context context) {
        init(collection);
        this.context = context;
    }

    public ImageInformationExtractor(String collection) {
        init(collection);
        this.localCounters = new HashMap<>();
    }

    private void init(String collection) {
        this.collection = collection;
        entries = new HashMap<>();
        ImageIO.setUseCache(false);
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

    public void parseRecord(String arcName, String arcURL) {
        if (arcURL.endsWith("warc.gz") || arcURL.endsWith("warc")) {
            parseWarcEntryRecord(arcName, arcURL);
        } else {
            parseArcEntry(arcName, arcURL);
        }
    }

    public void parseWarcEntryRecord(String warcName, String arcURL) {
        ImageSearchIndexingUtil.readWarcRecords(arcURL, this, (record) -> {

            String mimetype = record.getContentMimetype();
            if (mimetype != null) {
                if (mimetype.contains("image")) {
                    createImageDB(arcURL, record, context, warcName, record.getWARCRecord().getHeader().getOffset());
                }
                if (mimetype.contains("html")) { /*only processing images*/
                    logger.debug("Searching images in html record");
                    parseImagesFromHtmlRecord(context, record.getContentBytes(), record.getWARCRecord().getHeader().getUrl(), record.getTs(), warcName, record.getWARCRecord().getHeader().getOffset());
                }
            }
        });

    }

    public void parseArcEntry(String warcName, String arcURL) {
        ImageSearchIndexingUtil.readArcRecords(arcURL, this, record -> {

            boolean isImage = record.getMetaData().getMimetype().contains("image");
            if (isImage) {
                createImageDB(arcURL, record, context, warcName, record.getMetaData().getOffset());
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
                parseImagesFromHtmlRecord(context, recordContentBytes, record.getHeader().getUrl(), record.getMetaData().getDate(), warcName, record.getMetaData().getOffset());
            }
        });

    }

    public ImageData saveImageMetadataInline(String url, String timestamp, Mapper.Context context, String warcName, long warcOffset) {
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

            return saveImageMetadata("hash:" + imageURLHashKey, imageURLHashKey, timestamp, reportedMimeType, contentBytes, context, warcName, warcOffset);
        } catch (Exception e) {
            logger.error(String.format("Malformed inline image"));
            return null;
        }
    }


    public ImageData saveImageMetadata(String url, String imageURLHashKey, String timestamp, String reportedMimeType, byte[] contentBytes, Mapper.Context context, String warcName, long warcOffset) {

        String imgSurt = WARCInformationParser.toSURT(url);


        String detectedMimeType = "";

        try {

            detectedMimeType = WARCInformationParser.getMimeType(contentBytes);

            if (detectedMimeType == null) {
                this.getCounter(ImageIndexerWithDupsJob.IMAGE_COUNTERS.IMAGES_IN_WARC_MIME_INVALID).increment(1);
                detectedMimeType = "";
            } else if (!detectedMimeType.isEmpty() && !detectedMimeType.equals(reportedMimeType)) {
                logger.debug(String.format("MimeType for http://arquivo.pt/wayback/%s/%s", timestamp, url));
                logger.debug(String.format("reported: \"%s\" ; detected: \"%s\"", reportedMimeType, detectedMimeType));
                this.getCounter(ImageIndexerWithDupsJob.IMAGE_COUNTERS.IMAGES_IN_WARC_MIME_WRONG).increment(1);
            }
        } catch (Exception e) {
            this.getCounter(ImageIndexerWithDupsJob.IMAGE_COUNTERS.IMAGES_IN_WARC_MIME_INVALID).increment(1);
        }

        ImageData imageData = new ImageData(imageURLHashKey, timestamp, url, imgSurt, reportedMimeType, detectedMimeType, this.collection, contentBytes, warcName, warcOffset);

        try {
            imageData = ImageParse.getPropImage(imageData);
        } catch (Exception | StackOverflowError e) {
            this.getCounter(ImageIndexerWithDupsJob.IMAGE_COUNTERS.IMAGES_IN_WARC_FAILED).increment(1);
            return null;
        }


        if (imageData == null) {
            this.getCounter(ImageIndexerWithDupsJob.IMAGE_COUNTERS.IMAGES_IN_WARC_FAILED).increment(1);
        } else if (url.startsWith("hash:") && (imageData.getWidth() < ImageParse.MIN_WIDTH || imageData.getHeight() < ImageParse.MIN_HEIGHT)) {
            this.getCounter(ImageIndexerWithDupsJob.IMAGE_COUNTERS.IMAGES_IN_WARC_TOO_SMALL_BASE64).increment(1);
        } else if (imageData.getWidth() < ImageParse.MIN_WIDTH || imageData.getHeight() < ImageParse.MIN_HEIGHT) {
            this.getCounter(ImageIndexerWithDupsJob.IMAGE_COUNTERS.IMAGES_IN_WARC_TOO_SMALL).increment(1);
        } else if (imageData.getWidth() * imageData.getHeight() > ImageParse.MAX_HEIGHT * ImageParse.MAX_HEIGHT) {
            this.getCounter(ImageIndexerWithDupsJob.IMAGE_COUNTERS.IMAGES_IN_WARC_TOO_LARGE).increment(1);
        } else {

            this.getCounter(ImageIndexerWithDupsJob.IMAGE_COUNTERS.IMAGES_IN_WARC_PARSED_DUP).increment(1);
            FullImageMetadata fullImageMetadata = entries.get(imageData.getSurt());
            if (fullImageMetadata == null) {
                fullImageMetadata = new FullImageMetadata();
                entries.put(imageData.getSurt(), fullImageMetadata);
            }
            boolean isNew = entries.get(imageData.getSurt()).addImageData(imageData);
            if (isNew)
                this.getCounter(ImageIndexerWithDupsJob.IMAGE_COUNTERS.IMAGES_IN_WARC_PARSED).increment(1);

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

    public void createImageDB(String arcURL, WARCRecordResponseEncapsulated record, Mapper.Context context, String warcName, long warcOffset) {
        String url = "";
        String timestamp = "";
        try {
            url = record.getWARCRecord().getHeader().getUrl();
            timestamp = record.getTs();
            String mime = record.getContentMimetype();

            String imageURLHashKey = ImageSearchIndexingUtil.md5ofString(url);
            byte[] contentBytes = null;

            this.getCounter(ImageIndexerWithDupsJob.IMAGE_COUNTERS.IMAGES_IN_WARC_TOTAL).increment(1);

            try {
                contentBytes = record.getContentBytes();
            } catch (RuntimeException e) {
                logger.error(String.format("Error getting record content bytes for image url: %s/%s with error message %s", timestamp, url, e.getMessage()));
                this.getCounter(ImageIndexerWithDupsJob.IMAGE_COUNTERS.IMAGES_IN_WARC_FAILED).increment(1);
                return;
            }

            saveImageMetadata(url, imageURLHashKey, timestamp, mime, contentBytes, context, warcName, warcOffset);

        } catch (Exception e) {
            logger.error(String.format("Error parsing image url: %s/%s with error message %s", timestamp, url, e.getMessage()));
            this.getCounter(ImageIndexerWithDupsJob.IMAGE_COUNTERS.IMAGES_IN_WARC_FAILED).increment(1);
        }
    }

    public ImageData createImageDB(String arcURL, ARCRecord record, Mapper.Context context, String warcName, long warcOffset) {
        String url = record.getHeader().getUrl();
        String timestamp = record.getMetaData().getDate();
        String mime = record.getMetaData().getMimetype();
        String imageURLHashKey = ImageSearchIndexingUtil.md5ofString(url);

        byte[] contentBytes;

        this.getCounter(ImageIndexerWithDupsJob.IMAGE_COUNTERS.IMAGES_IN_WARC_TOTAL).increment(1);

        try {
            contentBytes = ImageSearchIndexingUtil.getRecordContentBytes(record);
        } catch (IOException e) {
            logger.error(String.format("Error getting record content bytes for image url: %s/%s on offset %d with error message %s", timestamp, url, record.getBodyOffset(), e.getMessage()));
            this.getCounter(ImageIndexerWithDupsJob.IMAGE_COUNTERS.IMAGES_IN_WARC_FAILED).increment(1);
            return null;
        }

        return saveImageMetadata(url, imageURLHashKey, timestamp, mime, contentBytes, context, warcName, warcOffset);
    }

    public void parseImagesFromHtmlRecord(Mapper.Context context, byte[] arcRecordBytes, String pageURL, String
            pageTstamp, String warcName, long warcOffset) {
        try {
            logger.debug("Parsing Images from HTML in (W)ARCrecord");
            logger.debug("Read Content Bytes from (W)ARCrecord" + arcRecordBytes.length);
            logger.debug("URL: " + pageURL);
            logger.debug("Page TS: " + pageTstamp);


            String recordEncoding = ImageSearchIndexingUtil.guessEncoding(arcRecordBytes);
            InputStream is = new ByteArrayInputStream(arcRecordBytes);

            String html = IOUtils.toString(is, recordEncoding);

            Document doc = Jsoup.parse(html, pageURL);

            String pageTitle = doc.title(); /*returns empty string if no title in html document*/

            this.getCounter(ImageIndexerWithDupsJob.PAGE_COUNTERS.PAGES).increment(1);
            //Find all images in img tags
            Elements imgs = doc.getElementsByTag("img");
            int pageImages = imgs.size();

            //logger.debug("Page contains: " + pageImages + " images");

            this.getCounter(ImageIndexerWithDupsJob.PAGE_COUNTERS.IMAGES_IN_HTML_TOTAL).increment(pageImages);

            URL uri = new URL(pageURL);
            String pageHost = uri.getHost();
            String pageProtocol = uri.getProtocol();
            String pageURLTokens = getURLSrcTokens(pageURL);

            if (pageTstamp == null || pageTstamp.equals("")) {
                logger.debug("Null pageTstamp");
                pageTstamp = "";
            }

            logger.debug("pageTstamp:" + pageTstamp);

            Set<String> imgSrcParsed = new HashSet<>();

            int parsedImagesPerPage = 0;
            try {
                for (Element el : imgs) {

                    Set<String> imgSrcAtrToParse = getImgURLToParse(pageURL, el);

                    for (String imgRelSrc : imgSrcAtrToParse) {

                        String imgSrc = StringUtil.resolve(pageURL, imgRelSrc);

                        boolean alreadyFoundInPage = imgSrcParsed.contains(imgRelSrc);
                        imgSrcParsed.add(imgRelSrc);

                        logger.debug("Getting information for: " + imgSrc);
                        if (imgRelSrc.startsWith(DATA_IMAGE_URL_PREFIX)) {
                            logger.debug("Inline image");
                            ImageData acceptedRecord = saveImageMetadataInline(imgRelSrc, pageTstamp, context, warcName, warcOffset);
                            this.getCounter(ImageIndexerWithDupsJob.PAGE_COUNTERS.IMAGES_IN_HTML_BASE64).increment(1);
                            if (acceptedRecord == null)
                                continue;
                            imgSrc = acceptedRecord.getUrl();
                        } else if (imgSrc.length() > MAX_IMAGE_FIELD_SIZE || pageURL.length() > MAX_IMAGE_FIELD_SIZE) {
                            logger.debug("URL of image too big ");
                            logger.debug(pageURL.substring(0, 500) + "...");
                            this.getCounter(ImageIndexerWithDupsJob.PAGE_COUNTERS.IMAGES_IN_HTML_INVALID).increment(1);
                            continue;
                        } else if (imgRelSrc.isEmpty()) {
                            logger.debug("Null imgSrc");
                            this.getCounter(ImageIndexerWithDupsJob.PAGE_COUNTERS.IMAGES_IN_HTML_INVALID).increment(1);
                            continue;
                        }

                        this.getCounter(ImageIndexerWithDupsJob.PAGE_COUNTERS.IMAGES_IN_HTML_MATCHING).increment(1);

                        String imgSrcTokens = getURLSrcTokens(imgSrc);
                        String imgTitle = getHTMLAttribute(el, "title");
                        String imgAlt = getHTMLAttribute(el, "alt");

                        String imgCaption = extractCaptionFromParent(el);

                        insertImageIndexes(imgSrc, imgSrcTokens, imgTitle, imgAlt, imgCaption, pageImages, pageTstamp, pageURL, pageHost, pageProtocol, pageTitle, pageURLTokens, "img", warcName, warcOffset);

                        logger.debug("Written to file - successfully indexed image record");
                    }

                    parsedImagesPerPage++;

                    if (parsedImagesPerPage >= MAX_IMAGE_IN_HTML) {
                        this.getCounter(ImageIndexerWithDupsJob.PAGE_COUNTERS.IMAGES_IN_HTML_EXCEDED).increment(1);
                        this.getCounter(ImageIndexerWithDupsJob.PAGE_COUNTERS.IMAGES_IN_HTML_NOT_PARSED).increment(imgs.size() - parsedImagesPerPage);
                        break;
                    }
                }
            } catch (Exception e) {
                logger.error(String.format("Error parsing HTML img record: %s", e.getMessage()));
                this.getCounter(ImageIndexerWithDupsJob.PAGE_COUNTERS.IMAGES_IN_HTML_FAILED).increment(pageImages);
            }


            Elements links = doc.getElementsByTag("a");

            logger.debug("Page contains: " + links.size() + " links");

            try {

                for (Element el : links) {

                    String imgSrc = el.attr("abs:href");
                    String imgRelSrc = el.attr("href").trim();

                    imgSrcParsed.add(imgRelSrc);
                    if (!isLinkToImage(imgSrc))
                        continue;

                    logger.debug("Getting information for: " + imgSrc);

                    if (imgSrc.length() > MAX_IMAGE_FIELD_SIZE || pageURL.length() > MAX_IMAGE_FIELD_SIZE) {
                        logger.debug("URL of image too big ");
                        logger.debug(pageURL.substring(0, 500) + "...");
                        this.getCounter(ImageIndexerWithDupsJob.PAGE_COUNTERS.IMAGES_IN_HTML_INVALID).increment(1);
                        continue;
                    } else if (imgRelSrc.isEmpty()) {
                        logger.debug("Null href");
                        this.getCounter(ImageIndexerWithDupsJob.PAGE_COUNTERS.IMAGES_IN_HTML_INVALID).increment(1);
                        continue;
                    }

                    this.getCounter(ImageIndexerWithDupsJob.PAGE_COUNTERS.IMAGES_IN_HTML_MATCHING).increment(1);
                    this.getCounter(ImageIndexerWithDupsJob.PAGE_COUNTERS.IMAGES_IN_HTML_MATCHING_LINK).increment(1);

                    String imgSrcTokens = getURLSrcTokens(imgSrc);


                    String imgCaption = el.text();
                    if (imgCaption.length() >= MAX_IMAGE_FIELD_SIZE) {
                        imgCaption = imgCaption.substring(0, MAX_IMAGE_FIELD_SIZE);
                    }


                    insertImageIndexes(imgSrc, imgSrcTokens, "", "", imgCaption, pageImages, pageTstamp, pageURL, pageHost, pageProtocol, pageTitle, pageURLTokens, "a", warcName, warcOffset);

                    logger.debug("Written to file - successfully indexed image record");

                }

            } catch (Exception e) {
                logger.error(String.format("Error parsing HTML img record: %s", e.getMessage()));
                this.getCounter(ImageIndexerWithDupsJob.PAGE_COUNTERS.IMAGES_IN_HTML_FAILED).increment(pageImages);
            }

            List<String> cssUrls = new LinkedList<>();
            Matcher m = CSS_URLS.matcher(html);
            while (m.find()) {
                String imgRelSrc = m.group(1);
                if (!imgRelSrc.isEmpty() && !imgSrcParsed.contains(imgRelSrc)) {
                    if (imgRelSrc.startsWith(DATA_IMAGE_URL_PREFIX)) {
                        cssUrls.add(imgRelSrc);
                    } else {
                        try {
                            String imgSrc = StringUtil.resolve(pageURL, imgRelSrc);
                            if (isLinkToImage(imgSrc)) {
                                cssUrls.add(imgRelSrc);
                            }

                        } catch (Exception ignored) {

                        }
                    }
                }
            }

            for (String imgRelSrc : cssUrls) {

                String imgSrc = StringUtil.resolve(pageURL, imgRelSrc);

                logger.debug("Getting information for: " + imgSrc);

                if (imgSrc.length() > MAX_IMAGE_FIELD_SIZE || pageURL.length() > MAX_IMAGE_FIELD_SIZE) {
                    logger.debug("URL of image too big ");
                    logger.debug(pageURL.substring(0, 500) + "...");
                    this.getCounter(ImageIndexerWithDupsJob.PAGE_COUNTERS.IMAGES_IN_HTML_INVALID).increment(1);
                    continue;
                } else if (imgRelSrc == null || imgRelSrc.equals("")) {
                    logger.debug("Null href");
                    this.getCounter(ImageIndexerWithDupsJob.PAGE_COUNTERS.IMAGES_IN_HTML_INVALID).increment(1);
                    continue;
                }

                this.getCounter(ImageIndexerWithDupsJob.PAGE_COUNTERS.IMAGES_IN_HTML_MATCHING).increment(1);
                this.getCounter(ImageIndexerWithDupsJob.PAGE_COUNTERS.IMAGES_IN_HTML_MATCHING_CSS).increment(1);

                String imgSrcTokens = getURLSrcTokens(imgSrc);

                insertImageIndexes(imgSrc, imgSrcTokens, "", "", "", pageImages, pageTstamp, pageURL, pageHost, pageProtocol, pageTitle, pageURLTokens, "css", warcName, warcOffset);

                logger.debug("Written to file - successfully indexed image record");

            }


            if (imgs.size() > 0)
                this.getCounter(ImageIndexerWithDupsJob.PAGE_COUNTERS.PAGES_WITH_IMAGES).increment(1);

        } catch (Exception e) {
            logger.debug("Something failed JSOUP parsing " + e.getMessage());
        }


    }

    private boolean isLinkToImage(String imgSrc) {

        String extension = "";
        try {
            URL url = new URL(imgSrc);
            extension = FilenameUtils.getExtension(url.getPath()).toLowerCase();
        } catch (Exception ignored) {
            return false;
        }

        return IMAGE_FILE_EXTENSIONS.contains(extension);
    }

    private String extractCaptionFromParent(Element node) {

        Element current = node;
        String imgCaption = "";

        int maxChildLevel = 0;
        int maxChildLevelCount = 0;
        int i = 0;
        while (current != null) {
            if (maxChildLevelCount <= current.childNodeSize()) {
                maxChildLevelCount = current.childNodeSize();
                maxChildLevel = i;
            }

            current = current.parent();
            i++;
        }

        i = 0;
        Element previous = node;
        current = node;
        // Go up the DOM tree until something is found or root is reached
        while (current != null && (imgCaption = current.text().trim()).isEmpty()) {
            previous = current;
            current = current.parent();
            i++;
        }

        if (i >= maxChildLevel) {
            Element sibling = previous.previousElementSibling();
            String imgCaptionPrev = "";
            String imgCaptionNext = "";
            while (sibling != null && (imgCaptionPrev = sibling.text().trim()).isEmpty()) {
                sibling = sibling.previousElementSibling();
            }

            sibling = previous.nextElementSibling();
            while (sibling != null && (imgCaptionNext = sibling.text().trim()).isEmpty()) {
                sibling = sibling.nextElementSibling();
            }

            imgCaption = (imgCaptionPrev + "\n" + imgCaptionNext).trim();
        }


        // No need for additional checks, as imgCaption will be empty if other conditions fail
        if (imgCaption.length() > MAX_PARENT_CAPTION_SIZE) {
            // Crop until closest empty space near the chosen text border (MAX_PARENT_CAPTION_SIZE chars in the end of the caption)
            int lastSpace = imgCaption.substring(0, MAX_PARENT_CAPTION_SIZE / 2).lastIndexOf(" ");
            if (lastSpace == -1)
                lastSpace = MAX_PARENT_CAPTION_SIZE / 2;

            String newImgCaption = imgCaption.substring(0, lastSpace).trim();

            lastSpace = (imgCaption.length() - MAX_PARENT_CAPTION_SIZE / 2) + imgCaption.substring(imgCaption.length() - MAX_PARENT_CAPTION_SIZE / 2).indexOf(" ");

            newImgCaption += "\n" + imgCaption.substring(lastSpace).trim();
            imgCaption = newImgCaption.trim();
        }
        return imgCaption;
    }

    private String getHTMLAttribute(Element el, String atr) {
        String atrVal = el.attr(atr);
        if (atrVal.length() >= MAX_IMAGE_FIELD_SIZE) {
            atrVal = atrVal.substring(0, MAX_IMAGE_FIELD_SIZE);
        }
        return atrVal;
    }

    private Set<String> getImgURLToParse(String pageURL, Element el) {
        Set<String> imgSrcAtrToParse = new HashSet<>();

        //Find all images in normal img src attributes
        for (String tag : IMAGE_TAG_ATTRIBUTES_WITH_FILES) {
            String imgRelSrc = el.attr(tag).trim();
            if (!imgRelSrc.isEmpty())
                imgSrcAtrToParse.add(imgRelSrc);
        }

        int oldSize = imgSrcAtrToParse.size();

        //Find text that matches URLs in all other img src attributes
        for (Attribute attribute : el.attributes()) {
            try {
                String imgRelSrc = attribute.getValue();
                if (imgRelSrc != null && !imgRelSrc.isEmpty()) {
                    if (imgRelSrc.startsWith(DATA_IMAGE_URL_PREFIX)) {
                        imgSrcAtrToParse.add(imgRelSrc);
                    } else {
                        String imgSrc = StringUtil.resolve(pageURL, imgRelSrc);
                        if (isLinkToImage(imgSrc)) {
                            imgSrcAtrToParse.add(imgRelSrc);
                        }
                    }
                }
            } catch (Exception ignored) {
                //Crashes if attribute is not an URL, safe to ignore
            }
        }

        this.getCounter(ImageIndexerWithDupsJob.PAGE_COUNTERS.IMAGES_IN_HTML_MATCHING_ALT_ATRIB).increment(imgSrcAtrToParse.size() - oldSize);

        return imgSrcAtrToParse;
    }

    private String getURLSrcTokens(String imgSrc) throws UnsupportedEncodingException {
        try {
            imgSrc = URLDecoder.decode(imgSrc, "UTF-8"); /*Escape imgSrc URL e.g %C3*/
        } catch (Exception ignored) {

        }
        return ImageSearchIndexingUtil.parseURL(imgSrc);
    }

    private void insertImageIndexes(String imgSrc, String imgSrcTokens, String imgTitle, String imgAlt,
                                    String imgCaption, int pageImages, String pageTstamp, String pageURL, String pageHost, String pageProtocol, String
                                            pageTitle, String pageURLTokens, String foundInTag, String warc, long warcOffset) {
        String imgSurtSrc = WARCInformationParser.toSURT(imgSrc);

        PageImageData pageImageData = new PageImageData("page", imgTitle, imgAlt, imgSrcTokens, imgCaption, pageTitle, pageURLTokens, imgSrc, imgSurtSrc, pageImages, pageTstamp, pageURL, pageHost, pageProtocol, foundInTag, warc, warcOffset, collection);

        this.getCounter(ImageIndexerWithDupsJob.PAGE_COUNTERS.IMAGES_IN_HTML_SENT_DUP).increment(1);

        FullImageMetadata fullImageMetadata = entries.get(pageImageData.getImgSurt());
        if (fullImageMetadata == null) {
            fullImageMetadata = new FullImageMetadata();
            entries.put(pageImageData.getImgSurt(), fullImageMetadata);
        }

        boolean isNew = entries.get(pageImageData.getImgSurt()).addPageImageData(pageImageData);
        if (isNew) {
            this.getCounter(ImageIndexerWithDupsJob.PAGE_COUNTERS.IMAGES_IN_HTML_SENT).increment(1);
        }
    }

    public HashMap<String, FullImageMetadata> getEntries() {
        return entries;
    }

}
