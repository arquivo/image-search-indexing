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
import org.checkerframework.checker.units.qual.t;

import pt.arquivo.imagesearch.indexing.processors.ImageInformationExtractor;
import pt.arquivo.imagesearch.indexing.utils.ImageSearchIndexingUtil;
import pt.arquivo.imagesearch.indexing.utils.WARCInformationParser;

import org.slf4j.Logger;

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
     * Mime type detected
     */
    private String mimeTypeDetected;

    /**
     * Mime type reported
     */
    private String mimeTypeReported;

    /**
     * title
     */
    private String title;

    /**
     * metadata
     */
    private String metadata;

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
     *  content
     */
    private String content;

    /** 
     * Outlinks
     */
    private Set<Outlink> outlinks;

    /**
     * Digest of the document
     */
    private String digestContainer;

    /**
     * Digest of the content
     */
    private String digestContent;

    public TextDocumentData() {
        this.outlinks = new HashSet<>();
    }

    public TextDocumentData(String URL, String title, String timestampString, String content, String mimeTypeReported, String mimeTypeDetected, String warc, long warcOffset, String collection, String digestContainer, String digestContent) {
        setURL(URL);
        this.title = title;
        this.timestampString = timestampString;
        this.content = content;
        this.mimeTypeReported = mimeTypeReported;
        this.mimeTypeDetected = mimeTypeDetected;
        this.warc = warc;
        this.warcOffset = warcOffset;
        this.collection = collection;
        this.outlinks = new HashSet<>();
        this.digestContainer = digestContainer;
        this.digestContent = digestContent;
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
        return digestContent;
        //return timestampString + "/" + ImageSearchIndexingUtil.md5ofString(url);
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

    public String getMimeTypeDetected() {
        return mimeTypeDetected;
    }

    public String getMimeTypeReported() {
        return mimeTypeReported;
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

    public String getTimestampFormatted() {
        return String.format("%04d-%02d-%02dT%02d:%02d:%02dZ",
                timestamp.getYear(),
                timestamp.getMonthValue(),
                timestamp.getDayOfMonth(),
                timestamp.getHour(),
                timestamp.getMinute(),
                timestamp.getSecond());
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

    public String getContent() {
        return content;
    }

    public Set<Outlink> getOutlinks() {
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

    public void setMimeTypeDetected(String mimeTypeDetected) {
        this.mimeTypeDetected = mimeTypeDetected;
    }

    public void setMimeTypeReported(String mimeTypeReported) {
        this.mimeTypeReported = mimeTypeReported;
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

    public void setTimestamp(String TimestampString) {
        this.timestampString = TimestampString;
        // timestampString format is YYYYMMDDHHmmss
        if (TimestampString.length() < 14) {
            TimestampString = TimestampString.concat("00000000000000").substring(0, 14);
        }
        this.timestamp = LocalDateTime.of(
                Integer.parseInt(TimestampString.substring(0, 4)),
                Integer.parseInt(TimestampString.substring(4, 6)),
                Integer.parseInt(TimestampString.substring(6, 8)),
                Integer.parseInt(TimestampString.substring(8, 10)),
                Integer.parseInt(TimestampString.substring(10, 12)),
                Integer.parseInt(TimestampString.substring(12, 14))
        );
    }
    
    public void setDigestContainer(String digestContainer) {
        this.digestContainer = digestContainer;
    }

    public void setDigestContent(String digestContent) {
        this.digestContent = digestContent;
    }

    public String getDigestContainer() {
        return digestContainer;
    }

    public String getDigestContent() {
        return digestContent;
    }

    public void setMetadata(String metadata) {
        this.metadata = metadata;
    }

    public String getMetadata() {
        return metadata;
    }

    public void setURL(String url) {
        this.url = url;
        if (!url.startsWith("http"))
            url = "http://" + url;
        URL uri;
        try {
            uri = new URL(url);
            this.host = uri.getHost();
        } catch (MalformedURLException e) {
            // e.printStackTrace();
            url = url.replaceAll("http://", "").replaceAll("https://", "");
            this.host = url.split("/")[0];
        }
        this.urlTokens = ImageInformationExtractor.getURLSrcTokens(url);
        this.surt = WARCInformationParser.toSURT(url);
    }

    public void addOutlink(String outlink, String anchor) {
        String outlinkSurt = WARCInformationParser.toSURT(outlink);
        Outlink outlinkObj = new Outlink(outlinkSurt, outlink, anchor);
        this.outlinks.add(outlinkObj);
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
            this.mimeTypeDetected = other.mimeTypeDetected;
            this.mimeTypeReported = other.mimeTypeReported;
            this.title = other.title;
            this.urlTokens = other.urlTokens;
            this.timestamp = other.timestamp;

            this.timestampString = other.timestampString;
            this.url = other.url;
            this.surt = other.surt;
            this.host = other.host;
            this.content = other.content;
            this.outlinks = other.outlinks;
            this.digestContainer = other.digestContainer;
            this.digestContent = other.digestContent;
            this.metadata = other.metadata;

        } catch (ClassNotFoundException e) {
            System.err.println("Error reading TextDocumentData from Writable");
        }
    }
}

