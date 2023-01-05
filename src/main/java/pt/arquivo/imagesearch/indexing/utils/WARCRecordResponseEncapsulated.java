package pt.arquivo.imagesearch.indexing.utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.StatusLine;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tika.io.TikaInputStream;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCRecord;
import org.archive.util.LaxHttpParser;
import org.brotli.dec.BrotliInputStream;

/**
 * Class that helps parse extra header information from the WARCRecord
 */
public class WARCRecordResponseEncapsulated {
    public final Log LOG = LogFactory.getLog(WARCRecordResponseEncapsulated.class);

    private static final String TRANSFER_ENCODING = "transfer-encoding";
    private static final String CONTENT_ENCODING = "content-encoding";

    private static final String CHUNKED = "chunked";
    private static final String GZIPPED = "gzip";
    private static final String DEFLATE = "deflate";
    private static final String BROTLI = "br";


    private ArchiveRecord warcrecord;
    /**
     * Http status line object.
     * <p>
     * May be null if record is not http.
     */
    private StatusLine httpStatus = null;

    /**
     * Http headers.
     * <p>
     * Only populated after reading of headers.
     */
    private Header[] httpHeaders = null;

    /**
     * Status for this request.
     * <p>
     * There may be no status.
     */
    private String statusCode = null;

    /**
     * URL for the WARC
     */
    private String warcURL;

    /**
     * Map of record header fields.
     * <p>
     * We store all in a hashmap.  This way we can hold version 1 or
     * version 2 record meta pt.arquivo.imagesearch.indexing.data.
     *
     * <p>Keys are lowercase.
     */
    protected Map<String, Object> headerFields = null;


    /**
     * Creates WARCRecordResponse from WARCRecord
     *
     * @param warcrecord base WARC record
     * @param warcURL WARC URL
     * @throws IOException
     */
    public WARCRecordResponseEncapsulated(WARCRecord warcrecord, String warcURL)
            throws IOException {
        this.warcURL = warcURL;
        this.warcrecord = warcrecord;

        if (!isWARCResponseRecord()) {
            throw new InvalidWARCResponseIOException("Invalid WARCRecordResponse record");
        } else {
            readHttpHeader();
        }
    }

    /**
     * Creates WARCRecordResponse from WARCRecord and headers
     *
     * @param warcrecord base WARC record
     * @param headerFields parsed headers
     * @param warcURL WARC URL
     */
    public WARCRecordResponseEncapsulated(WARCRecord warcrecord, Map<String, Object> headerFields, String warcURL) {
        this.warcURL = warcURL;
        this.warcrecord = warcrecord;
        this.headerFields = headerFields;
    }


    private void readHttpHeader() throws IOException {
        String statusLinestr = LaxHttpParser.readLine(warcrecord, WARCRecord.WARC_HEADER_ENCODING);

        try {
            this.httpStatus = new StatusLine(statusLinestr);
            this.statusCode = String.valueOf(this.httpStatus.getStatusCode());
        } catch (HttpException e) {
            LOG.error("HttpException parsing statusCode isIndex ", e);
        } catch (Exception e) {
            LOG.error("Exception parsing statusCode isIndex ", e);
        }

        this.httpHeaders = LaxHttpParser.parseHeaders(warcrecord, WARCRecord.WARC_HEADER_ENCODING);

        processHttpHeaders();

    }

    /*Headers Keys are all converted to lowercase to avoid inconsistencies*/
    private void processHttpHeaders() {
        headerFields = new HashMap<String, Object>();
        for (int j = 0; j < httpHeaders.length; j++) {
            final Header header = this.httpHeaders[j];
            headerFields.put(header.getName().toLowerCase(), header.getValue());
        }
    }

    /**
     * Cehck if record is well formed
     *
     * @return ture if well formed
     */
    public boolean isWARCResponseRecord() {
        String warcRecordMimetype = warcrecord.getHeader().getMimetype();
        String warcRecordType = (String) warcrecord.getHeader().getHeaderValue(WARCConstants.HEADER_KEY_TYPE);
        return warcRecordType != null && warcRecordMimetype != null && WARCConstants.WARCRecordType.response.toString().equals(warcRecordType.trim()) &&
                (warcRecordMimetype.trim().equals(WARCConstants.HTTP_RESPONSE_MIMETYPE) ||
                        warcRecordMimetype.trim().equals(WARCConstants.HTTP_RESPONSE_MIMETYPE.replaceAll("\\s", "")));
    }


    /**
     * Gets base WARCRecord
     *
     * @return WARCRecord as ArchiveRecord
     */
    public ArchiveRecord getWARCRecord() {
        return warcrecord;
    }

    /**
     * Get server reported MIME type
     *
     * @return mimetype The mimetype that is in the WARC metaline -- NOT the http
     * content-type content.
     */
    public String getContentMimetype() {
        return (String) headerFields.get(WARCRecord.MIMETYPE_FIELD_KEY);
    }

    /**
     * Get the WARCRecord content bytes
     *
     * @return byte array with content
     */
    public byte[] getContentBytes() {

        try {
            InputStream astream = warcrecord;
            byte[] results = IOUtils.toByteArray(astream);
            InputStream stream = new ByteArrayInputStream(results);

            if (results.length == 0)
                return results;

            String contentEncoding = (String) headerFields.get(CONTENT_ENCODING);

            if (contentEncoding != null && contentEncoding.toLowerCase().contains(BROTLI)) {
                stream = new BrotliInputStream(stream);
            }

            //if (transferEncoding != null && transferEncoding.toLowerCase().contains(CHUNKED))
            //    System.out.println("Chunked");

            results = IOUtils.toByteArray(TikaInputStream.get(stream));
            return results;
        } catch (IOException e) {
            // Some WARCS don't have the encoding headers and fail to decode brotli encoded content. This tries to fix that.
            if(e.getMessage().toLowerCase().contains("brotli")){
                try{
                    InputStream astream = warcrecord;
                    byte[] results = IOUtils.toByteArray(astream);
                    InputStream stream = new BrotliInputStream(new ByteArrayInputStream(results));
                    results = IOUtils.toByteArray(TikaInputStream.get(stream));
                    return results;
                } catch (IOException err) {
                    LOG.error(String.format("Error getting content byte for WARC, %s. Message: %s", this.warcURL, err.getMessage()));
                    throw new RuntimeException(String.format("Error getting content byte for WARC, %s. Message: %s", this.warcURL, err.getMessage()));
                }
            }
            LOG.error(String.format("Error getting content byte for WARC, %s. Message: %s", this.warcURL, e.getMessage()));
            throw new RuntimeException(String.format("Error getting content byte for WARC, %s. Message: %s", this.warcURL, e.getMessage()));
        }
    }

    /**
     * Gets WARC timestamp in Archive format
     *
     * @return timestamp in the yyyyMMddHHmmss format
     */
    public String getTs() {
        /*dateWarc in Format 2018-04-03T12:53:43Z */
        String dateWarc = warcrecord.getHeader().getDate();
        String year = "";
        String month = "";
        String day = "";
        String hour = "";
        String minute = "";
        String second = "";

        try {
            SimpleDateFormat thedate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", new Locale("pt", "PT"));
            thedate.parse(dateWarc);
            Calendar mydate = thedate.getCalendar();

            year += mydate.get(Calendar.YEAR);
            int monthInt = mydate.get(Calendar.MONTH) + 1;
            int dayInt = mydate.get(Calendar.DAY_OF_MONTH);
            int hourInt = mydate.get(Calendar.HOUR_OF_DAY);
            int minuteInt = mydate.get(Calendar.MINUTE);
            int secondInt = mydate.get(Calendar.SECOND);
            month = monthInt < 10 ? "0" + monthInt : "" + monthInt;
            day = dayInt < 10 ? "0" + dayInt : "" + dayInt;
            hour = hourInt < 10 ? "0" + hourInt : "" + hourInt;
            minute = minuteInt < 10 ? "0" + minuteInt : "" + minuteInt;
            second = secondInt < 10 ? "0" + secondInt : "" + secondInt;

        } catch (Exception e) {
            LOG.error("WARC getTS: error parsing date");
            return null;
        }
        return year + month + day + hour + minute + second;
    }
}