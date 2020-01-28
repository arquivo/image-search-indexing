import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.httpclient.ChunkedInputStream;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.StatusLine;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.archive.format.warc.WARCConstants;
import org.archive.io.warc.WARCRecord;
import org.archive.util.LaxHttpParser;

public class WARCRecordResponseEncapsulated {

    public final Log LOG = LogFactory.getLog(WARCRecordResponseEncapsulated.class);

    private static final String TRANSFER_ENCODING = "transfer-encoding";
    private static final String CHUNKED = "chunked";


    private WARCRecord warcrecord;
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

    public int contentBegin = 0;

    /**
     * Map of record header fields.
     * <p>
     * We store all in a hashmap.  This way we can hold version 1 or
     * version 2 record meta data.
     *
     * <p>Keys are lowercase.
     */
    protected Map<String, Object> headerFields = null;

    /**
     * Constructor.
     */
    public WARCRecordResponseEncapsulated(WARCRecord warcrecord)
            throws IOException {
        this.warcrecord = warcrecord;
        if (!isWARCResponseRecord()) {
            throw new InvalidWARCResponseIOException("Invalid WARCRecordResponse record");
        } else {
            readHttpHeader();
        }
    }

    public WARCRecordResponseEncapsulated(WARCRecord warcrecord, Map<String, Object> headerFields)
            throws IOException {
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

    public boolean isWARCResponseRecord() {
        String warcRecordMimetype = warcrecord.getHeader().getMimetype();
        String warcRecordType = (String) warcrecord.getHeader().getHeaderValue(WARCConstants.HEADER_KEY_TYPE);
        if (warcRecordType == null || warcRecordMimetype == null || !WARCConstants.WARCRecordType.response.toString().equals(warcRecordType.trim()) ||
                !(warcRecordMimetype.trim().equals(WARCConstants.HTTP_RESPONSE_MIMETYPE) ||
                        warcRecordMimetype.trim().equals(WARCConstants.HTTP_RESPONSE_MIMETYPE.replaceAll("\\s", "")))) {
            return false;
        }
        return true;
    }


    public WARCRecord getWARCRecord() {
        return warcrecord;
    }

    /**
     * @return mimetype The mimetype that is in the WARC metaline -- NOT the http
     * content-type content.
     */
    public String getContentMimetype() {
        return (String) headerFields.get(warcrecord.MIMETYPE_FIELD_KEY);
    }

    public String getStatusCode() {
        return statusCode;
    }

    public boolean hasErrors() {
        // TODO create hasErrors method
        return false;
    }

    public HashMap<String, Object> getErrors() {
        // TODO Create getErrors method
        return null;
    }

    public String getStringContent() {
        try {
            return IOUtils.toString(warcrecord);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public byte[] getContentBytes() {
        try {
            String transferEncoding = (String) headerFields.get(TRANSFER_ENCODING);
            if (transferEncoding != null && transferEncoding.toLowerCase().contains(CHUNKED)) {
                /*Deal with chunked Record*/

                LOG.debug("Chunked Bytes");
                return getByteArrayFromInputStreamChunked(warcrecord);
            }
            /*Default case convert to byte array*/
            return IOUtils.toByteArray(warcrecord);
        } catch (IOException e) {
            throw new RuntimeException("Error getting content byte for WARC", e);
        }
    }

    private static byte[] getByteArrayFromInputStreamChunked(InputStream is) {
        ChunkedInputStream cis = null;
        int currentChar;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        byte[] unchunkedData = null;

        try {
            cis = new ChunkedInputStream(is);

            // read till the end of the stream
            while ((currentChar = cis.read(buffer)) != -1) {
                bos.write(buffer, 0, currentChar);
            }
            unchunkedData = bos.toByteArray();
            bos.close();
        } catch (IOException e) {
            // if any I/O error occurs
            e.printStackTrace();
        } finally {
            // releases any system resources associated with the stream
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (cis != null) {
                try {
                    cis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return unchunkedData;
    }

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