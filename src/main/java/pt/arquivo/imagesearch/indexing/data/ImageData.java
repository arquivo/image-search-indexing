package pt.arquivo.imagesearch.indexing.data;

import pt.arquivo.imagesearch.indexing.utils.WARCInformationParser;

import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;

public class ImageData {
    private static final int MAX_ADD_THRESHOLD = 50;
    private String imageHashKey;

    private List<LocalDateTime> timestamp;
    private List<String> timestampOriginalFormat;
    private List<String> contentHash;

    private String url;
    private String surt;
    private String mimeReported;
    private String mimeDetected;
    private String collection;

    private byte[] bytes;

    private int width;
    private int height;
    private int size;

    private int matchingImages;


    public ImageData(String imageHashKey, String timestamp, String url, String surt, String mimeReported, String mimeDetected, String collection, byte[] bytes, int matchingImages) {
        this.imageHashKey = imageHashKey;
        this.url = url;
        this.surt = surt;
        this.mimeReported = mimeReported;
        this.mimeDetected = mimeDetected;
        this.collection = collection;
        this.bytes = bytes;
        this.size = bytes.length;

        this.timestampOriginalFormat = new LinkedList<>();
        this.timestamp = new LinkedList<>();

        this.timestamp.add(WARCInformationParser.parseLocalDateTime(timestamp));
        this.timestampOriginalFormat.add(timestamp);
        this.contentHash = new LinkedList<>();

        this.matchingImages = matchingImages;
    }

    @Override
    public String toString() {
        return String.format("\"%s\": %s", mimeReported, url);
    }

    public String getImageHashKey() {
        return imageHashKey;
    }

    public void setImageHashKey(String imageHashKey) {
        this.imageHashKey = imageHashKey;
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

    public List<String> getContentHash() {
        return contentHash;
    }

    public void addContentHash(String contentHash) {
        if (this.contentHash.size() <= MAX_ADD_THRESHOLD && !this.contentHash.contains(contentHash))
            this.contentHash.add(contentHash);
    }

    public void addContentHashes(List<String> contentHashes) {
        for (String contentHash : contentHashes)
            addContentHash(contentHash);
    }

    public void addTimestampsString(List<String> timestamps) {
        for (String timestamp : timestamps)
            if (this.timestampOriginalFormat.size() <= MAX_ADD_THRESHOLD && !this.timestampOriginalFormat.contains(timestamp))
                this.timestampOriginalFormat.add(timestamp);
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

    public void addTimestamps(List<LocalDateTime> timestamps) {
        for (LocalDateTime timestamp : timestamps)
            if (this.timestamp.size() <= MAX_ADD_THRESHOLD && !this.timestamp.contains(timestamp))
                this.timestamp.add(timestamp);
    }

    public void addImageData(ImageData other) {
        this.addTimestamps(other.getTimestamp());
        this.addTimestampsString(other.getTimestampOriginalFormat());
        this.addContentHashes(other.getContentHash());
    }

    public List<String> getTimestampsAsStrings() {
        List<String> results = new LinkedList<>();
        for (LocalDateTime time : this.timestamp)
            results.add(time.toString());
        return results;
    }
}
