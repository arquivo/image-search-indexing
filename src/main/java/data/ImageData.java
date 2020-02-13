package data;

import utils.WARCInformationParser;

import java.time.LocalDateTime;

public class ImageData implements Comparable<LocalDateTime> {
    private String imageHashKey;

    private LocalDateTime timestamp;

    private String url;
    private String surt;
    private String mimeReported;
    private String mimeDetected;
    private String collection;
    private String contentHash;
    private byte[] bytes;

    private String timestampOriginalFormat;

    private int width;
    private int height;
    private int size;


    public ImageData(String imageHashKey, String timestamp, String url, String surt, String mimeReported, String mimeDetected, String collection, byte[] bytes) {
        this.imageHashKey = imageHashKey;
        this.url = url;
        this.surt = surt;
        this.mimeReported = mimeReported;
        this.mimeDetected = mimeDetected;
        this.collection = collection;
        this.bytes = bytes;
        this.size = bytes.length;

        this.timestampOriginalFormat = timestamp;
        this.timestamp = WARCInformationParser.parseLocalDateTime(timestamp);
    }

    @Override
    public String toString() {
        return String.format("\"%s\": %s", mimeReported, url);
    }

    @Override
    public int compareTo(LocalDateTime timestamp) {
        return this.timestamp.compareTo(timestamp);
    }


    public String getImageHashKey() {
        return imageHashKey;
    }

    public void setImageHashKey(String imageHashKey) {
        this.imageHashKey = imageHashKey;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getSurt() {
        return surt;
    }

    public void setSurt(String surt) {
        this.surt = surt;
    }

    public String getMimeReported() {
        return mimeReported;
    }

    public void setMimeReported(String mimeReported) {
        this.mimeReported = mimeReported;
    }

    public String getMimeDetected() {
        return mimeDetected;
    }

    public void setMimeDetected(String mimeDetected) {
        this.mimeDetected = mimeDetected;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public String getContentHash() {
        return contentHash;
    }

    public void setContentHash(String contentHash) {
        this.contentHash = contentHash;
    }

    public int getWidth() {
        return width;
    }

    public void setWidth(int width) {
        this.width = width;
    }

    public int getHeight() {
        return height;
    }

    public void setHeight(int height) {
        this.height = height;
    }

    public byte[] getBytes() {
        return this.bytes;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }

    public String getTimestampOriginalFormat() { return timestampOriginalFormat; }

    public String getURLWithTimestamp() { return timestampOriginalFormat + "/" + this.url; }

    public int getSize() {
        return size;
    }
}
