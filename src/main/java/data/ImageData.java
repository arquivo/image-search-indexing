package data;

import utils.WARCInformationParser;

import java.time.LocalDateTime;

public class ImageData implements Comparable<LocalDateTime> {
    private String imageHashKey;

    private String tstamp;

    private LocalDateTime timestamp;

    private String url;
    private String surt;
    private String mime;
    private String collection;
    private String contentHash;
    private String bytes64String;


    public ImageData(String imageHashKey, String tstamp, String url, String surt, String mime, String collection, String contentHash, String bytes64String) {
        this.imageHashKey = imageHashKey;
        this.tstamp = tstamp;
        this.url = url;
        this.surt = surt;
        this.mime = mime;
        this.collection = collection;
        this.contentHash = contentHash;
        this.bytes64String = bytes64String;

        this.timestamp = WARCInformationParser.parseLocalDateTime(tstamp);

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

    public String getTstamp() {
        return tstamp;
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

    public String getBytes64String() {
        return bytes64String;
    }
}
