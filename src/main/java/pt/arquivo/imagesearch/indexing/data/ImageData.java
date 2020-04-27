package pt.arquivo.imagesearch.indexing.data;

import pt.arquivo.imagesearch.indexing.utils.WARCInformationParser;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static pt.arquivo.imagesearch.indexing.utils.WARCInformationParser.getLocalDateTimeToTimestamp;

public class ImageData implements Serializable {
    private String imageURLHash;
    private String contentHash;

    private List<LocalDateTime> timestamp;
    private List<String> timestampOriginalFormat;

    private String url;
    private String surt;
    private String mimeReported;
    private String mimeDetected;
    private String collection;

    private byte[] bytes;

    private int width;
    private int height;
    private int size;

    public ImageData(String imageURLHash, String timestamp, String url, String surt, String mimeReported, String mimeDetected, String collection, byte[] bytes) {
        this.imageURLHash = imageURLHash;
        this.url = url;
        this.surt = surt;
        this.mimeReported = mimeReported;
        this.mimeDetected = mimeDetected;
        this.collection = collection;
        this.bytes = bytes;
        this.size = bytes.length;

        this.timestampOriginalFormat = new LinkedList<>();
        this.timestamp = new LinkedList<>();

        this.timestampOriginalFormat.add(timestamp);
        this.timestamp.add(WARCInformationParser.parseLocalDateTime(timestamp));
        this.contentHash = "";

    }

    @Override
    public String toString() {
        return String.format("\"%s\": %s", mimeReported, url);
    }

    public String getImageURLHash() {
        return imageURLHash;
    }

    public void setImageURLHash(String imageURLHash) {
        this.imageURLHash = imageURLHash;
    }

    public List<LocalDateTime> getTimestamp() {
        return timestamp;
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

    public List<String> getTimestampOriginalFormat() {
        return timestampOriginalFormat;
    }

    public String getURLWithTimestamp() {
        return timestampOriginalFormat.get(0) + "/" + this.url;
    }

    public int getSize() {
        return size;
    }

    public int getMatchingImages() {
        return this.timestamp.size();
    }

    public String setContentHash(String contentHash) {
        return this.contentHash = contentHash;
    }

    public List<String> getTimestampsAsStrings() {
        List<String> results = new LinkedList<>();
        for (LocalDateTime time : this.timestamp)
            results.add(time.toString());
        return results;
    }

    public boolean equals(ImageData o) {
        return (o.getContentHash().equals(this.getContentHash()) && o.getSurt().equals(this.getSurt()));
    }

    public void addTimestamp(ImageData imageData) {
        this.timestamp.addAll(imageData.timestamp);
        for (LocalDateTime localDT: imageData.timestamp)
            this.timestampOriginalFormat.add(getLocalDateTimeToTimestamp(localDT));
        Collections.sort(timestamp);
        Collections.sort(timestampOriginalFormat);
    }

    public String getId() {
        return timestampOriginalFormat.get(0) + "/" + imageURLHash;
    }
}
