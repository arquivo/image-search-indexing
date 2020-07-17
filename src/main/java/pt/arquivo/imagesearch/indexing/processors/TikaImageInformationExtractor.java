package pt.arquivo.imagesearch.indexing.processors;

import com.ibm.icu.text.CharsetDetector;
import com.ibm.icu.text.CharsetMatch;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.LinkContentHandler;
import org.apache.tika.sax.ToXMLContentHandler;
import org.apache.tika.sax.XHTMLContentHandler;
import org.apache.tika.sax.xpath.Matcher;
import org.apache.tika.sax.xpath.MatchingContentHandler;
import org.apache.tika.sax.xpath.XPathParser;

import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import pt.arquivo.imagesearch.indexing.ImageIndexerWithDupsJob;
import pt.arquivo.imagesearch.indexing.data.FullImageMetadata;
import pt.arquivo.imagesearch.indexing.data.ImageData;
import pt.arquivo.imagesearch.indexing.utils.ImageParse;
import pt.arquivo.imagesearch.indexing.utils.ImageSearchIndexingUtil;
import pt.arquivo.imagesearch.indexing.utils.WARCInformationParser;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class TikaImageInformationExtractor extends ImageInformationExtractor {

    private Logger logger = Logger.getLogger(TikaImageInformationExtractor.class);

    public TikaImageInformationExtractor(String collection, Mapper.Context context) {
        super(collection, context);
    }

    public TikaImageInformationExtractor(String collection) {
        super(collection);
    }


    public void parseImagesFromHtmlRecord(Mapper.Context context, byte[] arcRecordBytes, String pageURL, String
            pageTstamp, String warcName, long warcOffset) {
        try {
            logger.debug("Parsing Images from HTML in (W)ARCrecord");
            logger.debug("Read Content Bytes from (W)ARCrecord" + arcRecordBytes.length);
            logger.debug("URL: " + pageURL);
            logger.debug("Page TS: " + pageTstamp);

            if (pageURL.contains("www.srcoronado.com/primeiro-ministro-inaugurou-creche-em-s-romao/"))
                System.out.println("ij");

            String recordEncoding = TikaImageInformationExtractor.guessEncoding(arcRecordBytes);

            ByteArrayInputStream is = new ByteArrayInputStream(arcRecordBytes);

            // ContentHandler handler = new LinkContentHandler();
            ContentHandler handler = new BodyContentHandler();
            AutoDetectParser parser = new AutoDetectParser();
            Metadata metadata = new Metadata();
            parser.parse(is, handler, metadata);


            String html = IOUtils.toString(is, recordEncoding);



        } catch (SAXException e) {
            e.printStackTrace();
        } catch (TikaException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
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

        ByteArrayInputStream is = new ByteArrayInputStream(contentBytes);

        // ContentHandler handler = new LinkContentHandler();
        ContentHandler handler = new BodyContentHandler();
        AutoDetectParser parser = new AutoDetectParser();
        Metadata metadata = new Metadata();
        try {
            parser.parse(is, handler, metadata);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (TikaException e) {
            e.printStackTrace();
        }
        //System.out.println(handler.toString());

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

    public static String guessEncoding(byte[] bytes) {
        String DEFAULT_ENCODING = "UTF-8";
        InputStream bis = new ByteArrayInputStream(bytes);
        CharsetDetector cd = new CharsetDetector();
        try {
            cd.setText(bis);

            CharsetMatch cm = cd.detect();

            if (cm != null) {
                return cm.getName();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return DEFAULT_ENCODING;
    }
}
