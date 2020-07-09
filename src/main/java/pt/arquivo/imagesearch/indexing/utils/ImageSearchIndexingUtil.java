package pt.arquivo.imagesearch.indexing.utils;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Consumer;

import pt.arquivo.imagesearch.indexing.ImageIndexerWithDupsJob;
import pt.arquivo.imagesearch.indexing.processors.ImageInformationExtractor;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.log4j.Logger;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.arc.ARCReader;
import org.archive.io.arc.ARCReaderFactory;
import org.archive.io.arc.ARCRecord;
import org.archive.io.warc.WARCReaderFactory;
import org.archive.io.warc.WARCRecord;

/**
 * Utility methos
 */
public class ImageSearchIndexingUtil {

    public static final int MAXIMUM_RECORD_SIZE_MB = 32;
    private static Logger logger = Logger.getLogger(ImageSearchIndexingUtil.class);

    public static String md5ofString(String content) {
        return DigestUtils.md5Hex(content);
    }

    public static void readArcRecords(String arcURL, ImageInformationExtractor context, Consumer<ARCRecord> consumer) {
        logger.debug("Reading ARC records for: " + arcURL);
        ARCReader reader;
        try {
            reader = ARCReaderFactory.get(arcURL);
        } catch (Exception e) {
            context.getCounter(ImageIndexerWithDupsJob.IMAGE_COUNTERS.WARCS_FAILED).increment(1);
            return;
        }

        int records = 0;
        int errors = 0;

        reader.setStrict(true);
        Iterator<ArchiveRecord> ii = reader.iterator();

        try {
            while (ii.hasNext()) {
                ARCRecord record;
                try {
                    record = (ARCRecord) ii.next();
                } catch (RuntimeException e) {
                    errors++;
                    // skip this record
                    context.getCounter(ImageIndexerWithDupsJob.IMAGE_COUNTERS.RECORD_NEXT_FAILED).increment(1);
                    logger.error("Exception reading next (W)ARC record", e);
                    throw e;
                }
                try {
                    consumer.accept(record);
                    context.getCounter(ImageIndexerWithDupsJob.IMAGE_COUNTERS.RECORDS_READ).increment(1);
                } catch (RuntimeException e) {
                    context.getCounter(ImageIndexerWithDupsJob.IMAGE_COUNTERS.RECORDS_FAILED).increment(1);
                    logger.error("Exception reading (W)ARC record", e);
                    errors++;
                }

                ++records;
                if (record.hasErrors()) {
                    errors += record.getErrors().size();
                }
            }
            logger.debug("records: " + records);
            logger.debug("errors: " + errors);
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    logger.debug("error closing ArchiveReader" + e.getMessage());
                }

            }
        } catch (RuntimeException e) {
            context.getCounter(ImageIndexerWithDupsJob.IMAGE_COUNTERS.WARCS_FAILED_STREAM).increment(1);
            logger.error("Exception reading WARC bytes, WARCNAME: " + arcURL + " " + e.getMessage());
            if (!e.getMessage().startsWith("Retried"))
                throw e;
        }

    }

    public static byte[] getRecordContentBytes(ARCRecord record) throws IOException {
        record.skipHttpHeader();/*Skipping http headers to only get the content bytes*/
        byte[] buffer = new byte[1024 * 16];
        int len = record.read(buffer, 0, buffer.length);
        ByteArrayOutputStream contentBuffer =
                new ByteArrayOutputStream(1024 * MAXIMUM_RECORD_SIZE_MB * 1000); /*Max record size: 32Mb*/
        contentBuffer.reset();
        while (len != -1) {
            contentBuffer.write(buffer, 0, len);
            len = record.read(buffer, 0, buffer.length);
        }
        record.close();
        return contentBuffer.toByteArray();
    }

    public static void readWarcRecords(String warcURL, ImageInformationExtractor context, Consumer<WARCRecordResponseEncapsulated> consumer) {
        logger.debug("Reading WARC records for: " + warcURL);
        ArchiveReader reader = null;
        try {
            reader = WARCReaderFactory.get(warcURL);
        } catch (Exception e) {
            context.getCounter(ImageIndexerWithDupsJob.IMAGE_COUNTERS.WARCS_FAILED).increment(1);
            logger.error("Exception starting reading WARC", e);
            return;
        }
        int records = 0;
        int errors = 0;
        reader.setStrict(true);
        Iterator<ArchiveRecord> ii = reader.iterator();

        try {
            while (ii.hasNext()) {
                WARCRecord warcRecord;
                try {
                    warcRecord = (WARCRecord) ii.next();
                    WARCRecordResponseEncapsulated record = parseWarcRecord(warcRecord, context);
                    if (record != null)
                        consumer.accept(record);
                } catch (RuntimeException re) {
                    context.getCounter(ImageIndexerWithDupsJob.IMAGE_COUNTERS.RECORD_NEXT_FAILED).increment(1);
                    errors++;
                    logger.error("Exception reading next WARC record", re);
                    throw re;
                }
            }
        } catch (RuntimeException e) {
            context.getCounter(ImageIndexerWithDupsJob.IMAGE_COUNTERS.WARCS_FAILED_STREAM).increment(1);
            logger.error("Exception reading WARC bytes, WARCNAME: " + warcURL + " " + e.getMessage());
            throw e;
        }

        logger.info("WARCS RECORDS READ: " + records + " ERRORS: " + errors);
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException e) {
                logger.debug("error closing ArchiveReader" + e.getMessage());
            }
        }

    }

    public static WARCRecordResponseEncapsulated parseWarcRecord(WARCRecord warcRecord, ImageInformationExtractor context){
        String warcRecordType = (String) warcRecord.getHeader().getHeaderValue(WARCConstants.HEADER_KEY_TYPE);
        String warcRecordMimetype = warcRecord.getHeader().getMimetype();
        String warcName = ((String) warcRecord.getHeader().getHeaderValue(WARCConstants.READER_IDENTIFIER_FIELD_KEY));

        try {
            if (warcRecordType.equalsIgnoreCase(WARCConstants.WARCRecordType.resource.toString())) {
                Map<String, Object> headers = new HashMap<>();
                headers.put(WARCConstants.CONTENT_LENGTH.toLowerCase(), String.valueOf(warcRecord.getHeader().getContentLength()));
                headers.put(WARCConstants.CONTENT_TYPE.toLowerCase(), warcRecordMimetype);
                headers.put(warcRecord.MIMETYPE_FIELD_KEY.toLowerCase(), warcRecordMimetype);

                context.getCounter(ImageIndexerWithDupsJob.IMAGE_COUNTERS.RECORDS_READ).increment(1);
                return new WARCRecordResponseEncapsulated(warcRecord, headers, warcName);

            } else {
                context.getCounter(ImageIndexerWithDupsJob.IMAGE_COUNTERS.RECORDS_READ).increment(1);
                return new WARCRecordResponseEncapsulated(warcRecord, warcName);
            }

        } catch (InvalidWARCResponseIOException e) {
            /* This is not a WARCResponse; skip */
        } catch (IOException e) {
            context.getCounter(ImageIndexerWithDupsJob.IMAGE_COUNTERS.RECORDS_FAILED).increment(1);
            logger.error("IO Exception reading WARCrecord WARCNAME: " + warcName + " " + e.getMessage());
        } catch (Exception e) {
            context.getCounter(ImageIndexerWithDupsJob.IMAGE_COUNTERS.RECORDS_FAILED).increment(1);
            logger.error("Exception reading WARCrecord WARCNAME: " + warcName + " " + e.getMessage());
        }
        return null;
    }

    public static String guessEncoding(byte[] bytes) {
        String DEFAULT_ENCODING = "UTF-8";
        org.mozilla.universalchardet.UniversalDetector detector =
                new org.mozilla.universalchardet.UniversalDetector(null);
        detector.handleData(bytes, 0, bytes.length);
        detector.dataEnd();
        String encoding = detector.getDetectedCharset();
        detector.reset();
        if (encoding == null) {
            encoding = DEFAULT_ENCODING;
        }
        return encoding;
    }

    //private static final Pattern VALID_PATTERN = Pattern.compile("[0-9A-Za-z]*");
    private static final String SPLIT_PATTERN = "[\\p{Punct}\b]+";


    public static String parseURL(String toParse) {
        if (toParse.startsWith("hash:"))
            return "";
        return cleanPunctuation(toParse);
    }

    public static String cleanPunctuation(String toParse) {
        return String.join(" ", toParse.split(SPLIT_PATTERN));
    }

    public static List<String> getTimestampStandardFormat(List<LocalDateTime> timestamps) {
        List<String> output = new LinkedList<>();
        for (LocalDateTime l : timestamps)
            output.add(l.toString());
        return output;
    }

    public static int getFileSize(URL url) {
        URLConnection conn = null;
        try {
            conn = url.openConnection();
            if (conn instanceof HttpURLConnection) {
                ((HttpURLConnection) conn).setRequestMethod("HEAD");
            }
            return conn.getContentLength();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (conn instanceof HttpURLConnection) {
                ((HttpURLConnection) conn).disconnect();
            }
        }
    }

    public static void saveFile(URL download, String fileName) throws IOException {

        ReadableByteChannel rbc = Channels.newChannel(download.openStream());

        FileOutputStream fos = new FileOutputStream(fileName);

        fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);

        fos.close();
    }

}
