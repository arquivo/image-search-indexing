import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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

import com.mongodb.ServerAddress;

/**
 * Utility methos
 */
public class ImageSearchIndexingUtil {

    private static Logger logger = Logger.getLogger(ImageSearchIndexingUtil.class);

    public static String md5ofString(String content) {
        return DigestUtils.md5Hex(content);
    }

    public static void readArcRecords(String arcURL, Consumer<ARCRecord> consumer) {
        logger.debug("Reading ARC records for: " + arcURL);
        ARCReader reader;
        try {
            reader = ARCReaderFactory.get(arcURL);
        } catch (Exception e) {
            logger.error("Exception starting reading ARC", e);
            return;
        }

        int records = 0;
        int errors = 0;
        for (Iterator<ArchiveRecord> ii = reader.iterator(); ii.hasNext(); ) {
            ARCRecord record;
            try {
                record = (ARCRecord) ii.next();
            } catch (RuntimeException e) {
                errors++;
                // skip this record
                logger.error("Exception reading next (W)ARC record", e);
                break;
            }
            try {
                consumer.accept(record);
            } catch (RuntimeException e) {
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
    }

    public static void readWarcRecords(String warcURL, Consumer<WARCRecordResponseEncapsulated> consumer) {
        logger.debug("Reading WARC records for: " + warcURL);
        ArchiveReader reader = null;
        try {
            reader = WARCReaderFactory.get(warcURL);
        } catch (Exception e) {
            logger.error("Exception starting reading WARC", e);
            return;
        }
        int records = 0;
        int errors = 0;

        for (Iterator<ArchiveRecord> ii = reader.iterator(); ii.hasNext(); ) {

            WARCRecord warcRecord;
            try {
                warcRecord = (WARCRecord) ii.next();
            } catch (RuntimeException re) {
                errors++;
                // skip this record
                logger.error("Exception reading next WARC record", re);
                break;
            }

            String warcRecordType = (String) warcRecord.getHeader().getHeaderValue(WARCConstants.HEADER_KEY_TYPE);
            String warcRecordMimetype = warcRecord.getHeader().getMimetype();
            WARCRecordResponseEncapsulated record = null;

            try {
                if (warcRecordType.equalsIgnoreCase(WARCConstants.WARCRecordType.resource.toString())) {
                    Map<String, Object> headers = new HashMap<>();
                    headers.put(WARCConstants.CONTENT_LENGTH.toLowerCase(), String.valueOf(warcRecord.getHeader().getContentLength()));
                    headers.put(WARCConstants.CONTENT_TYPE.toLowerCase(), warcRecordMimetype);
                    headers.put(warcRecord.MIMETYPE_FIELD_KEY.toLowerCase(), warcRecordMimetype);

                    record = new WARCRecordResponseEncapsulated(warcRecord, headers);
                    consumer.accept(record);
                } else {
                    record = new WARCRecordResponseEncapsulated(warcRecord);
                    consumer.accept(record);
                }
            } catch (InvalidWARCResponseIOException e) {
                /* This is not a WARCResponse; skip */
                errors++;
            } catch (IOException e) {
                logger.debug("IO Exception reading WARCrecord WARCNAME: " + warcURL + " " + e.getMessage());
                errors++;
            } catch (Exception e) {
                logger.debug("Exception reading WARCrecord WARCNAME: " + warcURL + " " + e.getMessage());
                errors++;
            }
            ++records;
            if (record != null && record.hasErrors()) {
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

    }

    public static List<ServerAddress> getMongoDBServerAddresses(String mongodbServers) {
        System.out.println("Using mongodb servers: " + mongodbServers);

        return Arrays.asList(mongodbServers.split(",")).stream().map(mongoServerStr -> {
            String[] ms = mongoServerStr.split(":");
            String server = ms[0];
            Integer port = Integer.valueOf(ms[1]);
            return new ServerAddress(server, port);
        }).collect(Collectors.toList());
    }

    public static byte[] getRecordContentBytes(ARCRecord record) throws IOException {
        record.skipHttpHeader();/*Skipping http headers to only get the content bytes*/
        byte[] buffer = new byte[1024 * 16];
        int len = record.read(buffer, 0, buffer.length);
        ByteArrayOutputStream contentBuffer =
                new ByteArrayOutputStream(1024 * 16 * 1000); /*Max record size: 16Mb*/
        contentBuffer.reset();
        while (len != -1) {
            contentBuffer.write(buffer, 0, len);
            len = record.read(buffer, 0, buffer.length);
        }
        record.close();
        return contentBuffer.toByteArray();
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

    private static final Pattern VALID_PATTERN = Pattern.compile("[0-9A-Za-z]*");

    public static String parseURL(String toParse) {
        String result = "";
        Matcher matcher = VALID_PATTERN.matcher(toParse);
        while (matcher.find()) {
            result += matcher.group().trim() + " ";
        }
        return result;
    }

}
