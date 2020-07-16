package pt.arquivo.imagesearch.indexing.processors;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.txt.CharsetDetector;
import org.apache.tika.parser.txt.CharsetMatch;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.LinkContentHandler;
import org.apache.tika.sax.ToXMLContentHandler;
import org.apache.tika.sax.XHTMLContentHandler;
import org.apache.tika.sax.xpath.Matcher;
import org.apache.tika.sax.xpath.MatchingContentHandler;
import org.apache.tika.sax.xpath.XPathParser;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import pt.arquivo.imagesearch.indexing.utils.ImageSearchIndexingUtil;

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

            if (pageURL.contains("www.maisportugal.com/sites-ver"))
                System.out.println("ij");

            String recordEncoding = TikaImageInformationExtractor.guessEncoding(arcRecordBytes);

            ByteArrayInputStream is = new ByteArrayInputStream(arcRecordBytes);

            // ContentHandler handler = new LinkContentHandler();
            ContentHandler handler = new BodyContentHandler();
            AutoDetectParser parser = new AutoDetectParser();
            Metadata metadata = new Metadata();
            parser.parse(is, handler, metadata);
            System.out.println(handler.toString());


            String html = IOUtils.toString(is, recordEncoding);



        } catch (SAXException e) {
            e.printStackTrace();
        } catch (TikaException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


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
