package pt.arquivo.imagesearch.indexing.data;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Writable;

import pt.arquivo.imagesearch.indexing.processors.ImageInformationExtractor;
import pt.arquivo.imagesearch.indexing.utils.ImageSearchIndexingUtil;
import pt.arquivo.imagesearch.indexing.utils.WARCInformationParser;

public class TextDocumentData implements Comparable<LocalDateTime>, Writable, Serializable {

    /**
     * Collection where to which this  matches
     */
    private String collection;

    /**
     * (W)ARC name
     */
    private String warc;

    /**
     * (W)ARC offset in bytes
     */
    private long warcOffset;

    /**
     * Encoding reported
     */
    private String encodingReported;

    /**
     * Encoding detected
     */
    private String encodingDetected;

    /**
     * Mime type detected
     */
    private String mimeTypeDetected;

    /**
     * Mime type reported
     */
    private String mimeTypeReported;


    /**
     * Always full for this object
     */
    private String type;


    /**
     * title
     */
    private String title;

    /**
     *  url split into word tokens
     */
    private String urlTokens;

    /**
     *  capture timestamp
     */
    private LocalDateTime timestamp;


    /**
     * Oldest capture timestamp in the yyyyMMddHHmmss format
     */
    private String timestampString;

    /**
     * Document URL
     */
    private String url;

    /**
     * Document SURT
     */
    private String surt;

    /**
     * Host of the matching 
     */
    private String host;

    /**
     * protocol (http vs. https)
     */
    private String protocol;

    /**
     *  content
     */
    private String content;

    /** 
     * Outlinks
     */
    private Set<String> outlinks;

    public TextDocumentData() {
        this.outlinks = new HashSet<>();
    }

    public TextDocumentData(String URL, String title, String timestampString, String content, String encodingReported, String encodingDetected, String mimeTypeReported, String mimeTypeDetected, String warc, long warcOffset, String collection) {
        setURL(URL);
        this.title = title;
        this.timestampString = timestampString;
        this.content = content;
        this.encodingReported = encodingReported;
        this.encodingDetected = encodingDetected;
        this.mimeTypeReported = mimeTypeReported;
        this.mimeTypeDetected = mimeTypeDetected;
        this.warc = warc;
        this.warcOffset = warcOffset;
        this.collection = collection;
        this.outlinks = new HashSet<>();
    }

    @Override
    public int compareTo(LocalDateTime timestamp) {
        return this.timestamp.compareTo(timestamp);
    }

    @Override
    public String toString() {
        return String.format("\"%s\": \"%s\", %s", title, url, timestamp.toString());
    }

    public String getId() {
        return timestampString + "/" + ImageSearchIndexingUtil.md5ofString(url);
    }

    public String getURLHash() {
        return ImageSearchIndexingUtil.md5ofString(url);
    }

    // generate getters and setters
    public String getCollection() {
        return collection;
    }

    public String getWarc() {
        return warc;
    }

    public long getWarcOffset() {
        return warcOffset;
    }

    public String getEncodingReported() {
        return encodingReported;
    }

    public String getEncodingDetected() {
        return encodingDetected;
    }

    public String getMimeTypeDetected() {
        return mimeTypeDetected;
    }

    public String getMimeTypeReported() {
        return mimeTypeReported;
    }

    public String getType() {
        return type;
    }

    public String getTitle() {
        return title;
    }

    public String getUrlTokens() {
        return urlTokens;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public String getTimestampString() {
        return timestampString;
    }

    public String getUrl() {
        return url;
    }

    public String getSurt() {
        return surt;
    }

    public String getHost() {
        return host;
    }

    public String getProtocol() {
        return protocol;
    }

    public String getContent() {
        return content;
    }

    public Set<String> getOutlinks() {
        return outlinks;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public void setWarc(String warc) {
        this.warc = warc;
    }

    public void setWarcOffset(long warcOffset) {
        this.warcOffset = warcOffset;
    }

    public void setEncodingReported(String encodingReported) {
        this.encodingReported = encodingReported;
    }

    public void setEncodingDetected(String encodingDetected) {
        this.encodingDetected = encodingDetected;
    }

    public void setMimeTypeDetected(String mimeTypeDetected) {
        this.mimeTypeDetected = mimeTypeDetected;
    }

    public void setMimeTypeReported(String mimeTypeReported) {
        this.mimeTypeReported = mimeTypeReported;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setTitle(String Title) {
        this.title = Title;
    }

    public void setContent(String Content) {
        this.content = Content;
    }

    public void setURLTokens(String URLTokens) {
        this.urlTokens = URLTokens;
    }

    public void setTimestamp(LocalDateTime Timestamp) {
        this.timestamp = Timestamp;
    }

    public void setTimestampString(String TimestampString) {
        this.timestampString = TimestampString;
    }

    public void setURL(String url) {
        this.url = url;
        if (!url.startsWith("http"))
            url = "http://" + url;
        URL uri;
        try {
            uri = new URL(url);
            this.host = uri.getHost();
            this.protocol = uri.getProtocol();
            this.urlTokens = ImageInformationExtractor.getURLSrcTokens(url);
            this.surt = WARCInformationParser.toSURT(url);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }

    }

    public void addOutlink(String outlink) {
        String outlinkSurt = WARCInformationParser.toSURT(outlink);
        this.outlinks.add(outlinkSurt);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(this);
        byte[] data = baos.toByteArray();
        int size = data.length;
        out.writeInt(size);
        out.write(data);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        byte[] data = new byte[size];
        in.readFully(data);
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        ObjectInputStream ois = new ObjectInputStream(bais);
        try {
            TextDocumentData other = (TextDocumentData) ois.readObject();

            this.collection = other.collection;
            this.warc = other.warc;
            this.warcOffset = other.warcOffset;
            this.encodingReported = other.encodingReported;
            this.encodingDetected = other.encodingDetected;
            this.mimeTypeDetected = other.mimeTypeDetected;
            this.mimeTypeReported = other.mimeTypeReported;
            this.type = other.type;
            this.title = other.title;
            this.urlTokens = other.urlTokens;
            this.timestamp = other.timestamp;

            this.timestampString = other.timestampString;
            this.url = other.url;
            this.surt = other.surt;
            this.host = other.host;
            this.protocol = other.protocol;
            this.content = other.content;
            this.outlinks = other.outlinks;

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}

