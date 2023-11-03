package pt.arquivo.imagesearch.indexing.processors;

import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCRecord;
import pt.arquivo.imagesearch.indexing.ImageIndexerWithDupsJob;
import pt.arquivo.imagesearch.indexing.LocalFullPageIndexer.PAGE_INDEXER_COUNTERS;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.apache.log4j.Logger;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.archive.io.arc.ARCRecord;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.xml.sax.SAXException;

import pt.arquivo.imagesearch.indexing.utils.ImageSearchIndexingUtil;
import pt.arquivo.imagesearch.indexing.utils.WARCRecordResponseEncapsulated;

import java.io.*;
import java.net.MalformedURLException;
import java.util.*;


/**
 * Auxiliary class to extract metadata from pages for page search
 */
public class PageInformationExtractor implements InformationExtractor {

    /**
     * File extensions to be included when extracting text
     */
    private static final Set<String> FILES_TO_PARSE_MIMETYPES = new HashSet<>(Arrays.asList("application/msword", "application/pdf", "application/postscript", "application/rss+xml", "application/vnd.ms-excel", "application/vnd.ms-powerpoint", "application/vnd.oasis.opendocument.text", "application/vnd.oasis.opendocument.text-template", "application/vnd.oasis.opendocument.text-master", "application/vnd.oasis.opendocument.text-web", "application/vnd.oasis.opendocument.presentation", "application/vnd.oasis.opendocument.presentation-template", "application/vnd.oasis.opendocument.spreadsheet", "application/vnd.oasis.opendocument.spreadsheet-template", "application/vnd.sun.xml.calc", "application/vnd.sun.xml.calc.template", "application/vnd.sun.xml.impress", "application/vnd.sun.xml.impress.template", "application/vnd.sun.xml.writer", "application/vnd.sun.xml.writer.template", "application/xhtml+xml", "application/x-bzip2", "application/x-gzip", "application/x-kword", "application/x-kspread", "application/x-shockwave-flash", "application/zip", "text/html", "text/plain", "text/richtext", "text/rtf", "text/sgml", "text/tab-separated-values", "text/xml"));

    private Logger logger = Logger.getLogger(PageInformationExtractor.class);

    /**
     * Collection name
     */
    protected String collection;

    /**
     * Hadoop context
     */
    private Mapper.Context context;

    /**
     * Stores the counters. This enables using this code both inside and outside Hadoop
     */
    private HashMap<Enum<?>, Counter> localCounters;

    private HashMap<String, Counter> tmpCounters;


    private TikaConfig config;

    /**
     * Constructor used for Hadoop
     *
     * @param collection collection name
     * @param context Hadoop context
     */
    public PageInformationExtractor(String collection, Mapper.Context context) {
        init(collection);
        this.context = context;
    }

    /**
     * Constructor used for local parser
     *
     * @param collection collection name
     */
    public PageInformationExtractor(String collection) {
        init(collection);
        this.localCounters = new HashMap<>();
        this.tmpCounters = new HashMap<>();
    }

    /**
     * Init with common code from both constructors
     *
     * @param collection Collection name
     */
    private void init(String collection) {
        this.collection = collection;

        ClassLoader classLoader = getClass().getClassLoader();
        try {
            config = new TikaConfig(classLoader.getResourceAsStream("tika-config.xml"));
        } catch (TikaException | IOException | SAXException e) {
            e.printStackTrace();
        }
    }

    /**
     * Gets the desired counters from either Hadoop or local Counter cache
     *
     * @param counterName name of the counter
     * @return desired counter
     */
    public Counter getCounter(Enum<?> counterName) {
        if (context != null) {
            return context.getCounter(counterName);
        } else {
            if (localCounters.get(counterName) == null)
                localCounters.put(counterName, new GenericCounter(counterName.name(), counterName.name()));
            return localCounters.get(counterName);
        }
    }


    /**
     * Generic entry point to parse wither WARCs or ARCs
     *
     * @param arcName name of the (W)ARCs
     * @param arcURL (W)ARCs url
     */
    public void parseRecord(String arcName, String arcURL) {
        if (arcURL.endsWith("warc.gz") || arcURL.endsWith("warc")) {
            parseWarcEntryRecord(arcName, arcURL);
        } else {
            parseArcEntry(arcName, arcURL);
        }
    }

    /**
     * Parse a WARC record
     *
     * @param warcName WARC name
     * @param warcURL WARC url
     */
    public void parseWarcEntryRecord(String warcName, String warcURL) {
        ImageSearchIndexingUtil.readWarcRecords(warcURL, this, (record) -> {
            parseWarcRecord(record, warcName);
        });

    }

    /**
     * Parse a WARC record inner method
     *
     * @param record WARC record object
     * @param warcName WARC name
     */
    public void parseWarcRecord(WARCRecordResponseEncapsulated record, String warcName) {
        String mimeType = record.getContentMimetype();
        if (this.tmpCounters.get(mimeType) == null)
            this.tmpCounters.put(mimeType, new GenericCounter(mimeType, mimeType));
        this.tmpCounters.get(mimeType).increment(1);
        if (mimeType != null) {
            if (FILES_TO_PARSE_MIMETYPES.contains(mimeType)){
                parseTextRecord(record, warcName);
            }
        }
    }

    /**
     * Parse a ARC record
     *
     * @param arcName ARC name
     * @param arcURL ARC url
     */
    public void parseArcEntry(String arcName, String arcURL) {
        ImageSearchIndexingUtil.readArcRecords(arcURL, this, record -> {
            parseArcRecord(record, arcName);
        });
    }

    /**
     * Parse a ARC record inner method
     *
     * @param record ARC record object
     * @param arcName ARC url
     */
    public void parseArcRecord(ARCRecord record, String arcName) {
        String mimeType = getMimeType(record);
        if (this.tmpCounters.get(mimeType) == null)
            this.tmpCounters.put(mimeType, new GenericCounter(mimeType, mimeType));
        this.tmpCounters.get(mimeType).increment(1);
        if (mimeType != null) {
            if (FILES_TO_PARSE_MIMETYPES.contains(mimeType)){
                try {
                    record.skipHttpHeader();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                parseTextRecord(record, arcName);
            }
        }
    }

    public void parseHTMLPage(String pageURL, String pageTstamp, String warcName, long warcOffset, String html) throws MalformedURLException, UnsupportedEncodingException {
        boolean malformedPageForCaptions = false;
        long startTime = System.nanoTime();

        Document doc = Jsoup.parse(html, pageURL);

        String pageTitle = doc.title(); /*returns empty string if no title in html document*/

        this.getCounter(ImageIndexerWithDupsJob.PAGE_COUNTERS.PAGES).increment(1);
        //Find all images in img tags


    }

    /**
     * Record parser for HDFS WARCs
     * @
     * param rec record to be processed
     */
    public void parseRecord(ArchiveRecord rec) {
        if (rec instanceof ARCRecord) {
            ARCRecord arcRecord = (ARCRecord) rec;
            String arcName = ((ARCRecord) rec).getMetaData().getArc();
            parseArcRecord(arcRecord, arcName);
        } else {
            WARCRecordResponseEncapsulated warcRecord = ImageSearchIndexingUtil.parseWarcRecord((WARCRecord) rec, this);
            String warcName = ((String) rec.getHeader().getHeaderValue(WARCConstants.READER_IDENTIFIER_FIELD_KEY));
            if (warcRecord != null)
                parseWarcRecord(warcRecord, warcName);
        }
    }

    /**
     * Parse a text record
     *
     * @param record record to be parsed
     * @param arcName name of the (W)ARC
     */

    public void parseTextRecord(WARCRecordResponseEncapsulated record, String arcName) {
        String mimeType = record.getContentMimetype();
        getCounter(mimeToCounter(mimeType)).increment(1);
        parseTextRecord(record.getWARCRecord(), mimeType);
    }



    public String parseTextRecord(InputStream stream, String mimeType) {
        try {
            if (stream.available() == 0)
                return "";
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        Parser parser = new AutoDetectParser(config);
        Metadata metadata = new Metadata();
        BodyContentHandler handler = new BodyContentHandler(-1);
        ParseContext context = new ParseContext();

        try {
            System.out.println("Input stream length: " + stream.available());
            parser.parse(stream, handler, metadata, context);
            System.out.println("Encoding: " + metadata.get("Content-Encoding"));
            System.out.println("MIME type: " + metadata.get("Content-Type"));
        } catch (IOException | SAXException | TikaException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.out.println("Error parsing text record");
        }
        String body = handler.toString();

        for (String key : metadata.names()) {
            String value = metadata.get(key);
            if (value != null) {
                if (key.equals("Content-Type")) {
                    mimeType = value.split(";")[0].trim();
                }
            }
        }
        if (!body.isEmpty())
            return body;


        return body;
    }



    // Get all counters
    public HashMap<Enum<?>, Counter> getCounters() {
        return localCounters;
    }

    public HashMap<String, Counter> getTmpCounters() {
        return tmpCounters;
    }

    public Enum<PAGE_INDEXER_COUNTERS> mimeToCounter(String mimeType) {
        String enumName = mimeType.replace("/", "_").replace("+", "_").replace("-", "_").replace(".", "_").replace(";", "_").replace("=", "_").replace(" ", "_");
        return PAGE_INDEXER_COUNTERS.valueOf(enumName);
    }


    public String getMimeType(ArchiveRecord rec) {
        String mimeType = null;
        if (rec instanceof ARCRecord) {
            mimeType = ((ARCRecord) rec).getMetaData().getMimetype();
        } else {
            mimeType = ((WARCRecord) rec).getHeader().getMimetype();
            
        }
        if (mimeType != null) {
            mimeType = mimeType.split(";")[0].trim();
        }
        return mimeType;

    }
}
