package data;

import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;
import utils.WARCInformationParser;

import java.time.LocalDateTime;

public class ImageData implements Comparable<LocalDateTime> {
    private String imageHashKey;

    private LocalDateTime timestamp;

    private String url;
    private String surt;
    private String mime;
    private String collection;
    private String contentHash;
    private String bytes;

    private int width;
    private int height;


    public ImageData(String imageHashKey, String timestamp, String url, String surt, String mime, String collection, String contentHash, byte[] bytes, int width, int height) {
        this.imageHashKey = imageHashKey;
        this.url = url;
        this.surt = surt;
        this.mime = mime;
        this.collection = collection;
        this.contentHash = contentHash;
        this.bytes = Base64.encode(bytes);

        this.width = width;
        this.height = height;

        this.timestamp = WARCInformationParser.parseLocalDateTime(timestamp);

    }

    public ImageData(String imageHashKey, String timestamp, String url, String surt, String mime, String collection, byte[] bytes) {
        this.imageHashKey = imageHashKey;
        this.url = url;
        this.surt = surt;
        this.mime = mime;
        this.collection = collection;
        this.bytes = Base64.encode(bytes);

        this.timestamp = WARCInformationParser.parseLocalDateTime(timestamp);
    }

    @Override
    public String toString() {
        return String.format("\"%s\": %s", mime, url);
    }

    @Override
    public int compareTo(LocalDateTime timestamp) {
        return this.timestamp.compareTo(timestamp);
    }

    public String getImageHashKey() {
        return imageHashKey;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public String getUrl() {
        return url;
    }

    public String getSurt() {
        return surt;
    }

    public String getMime() {
        return mime;
    }

    public String getCollection() {
        return collection;
    }

    public String getContentHash() {
        return contentHash;
    }

    public byte[] getBytes() {
        return Base64.decode(bytes);
    }

    public String getBytesBase64() {
        return bytes;
    }

    public void setHeight(int height) {
        this.height = height;
    }

    public void setWidth(int width) {
        this.width = width;
    }

    public void setImageHashKey(String imageHashKey) {
        this.imageHashKey = imageHashKey;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setSurt(String surt) {
        this.surt = surt;
    }

    public void setMime(String mime) {
        this.mime = mime;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public void setContentHash(String contentHash) {
        this.contentHash = contentHash;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = Base64.encode(bytes);
    }

    public void setBytes(String bytes) {
        this.bytes = bytes;
    }

    public int getWidth() {
        return width;
    }

    public int getHeight() {
        return height;
    }
}
