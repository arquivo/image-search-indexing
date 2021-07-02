package pt.arquivo.imagesearch.indexing.data;

import pt.arquivo.imagesearch.indexing.utils.WARCInformationParser;

import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static pt.arquivo.imagesearch.indexing.utils.WARCInformationParser.getLocalDateTimeToTimestamp;

/**
 *
 */
public class ImageData implements Serializable {


    /**
     * (W)ARC name
     */
    private String warc;

    /**
     * (W)ARC offset in bytes
     */
    private long warcOffset;

    /**
     * MD5 hash of the image URL
     */
    private String imageURLHash;

    /**
     * SHA-256 hash of the image byte content
     */
    private String contentHash;

    /**
     * Timestamp of records where this image was found
     */
    private List<LocalDateTime> timestamp;

    /**
     * Timestamp of records where this image was found in archive string format
     */
    private List<String> timestampOriginalFormat;

    /**
     * Image url
     */
    private String url;

    /**
     * Image SURT
     */
    private String surt;

    /**
     * Image MIME type as recorded by the (W)ARC
     */
    private String mimeReported;

    /**
     * Image MIME type as detected by ImageIO
     */
    private String mimeDetected;

    /**
     * Collection to which this image belongs
     */
    private String collection;

    /**
     * Image content bytes
     */
    private byte[] bytes;

    /**
     * Image width in pixels
     */
    private int width;

    /**
     * Image height in pixels
     */
    private int height;

    /**
     * Image size in bytes
     */
    private int size;

    /**
     * SURT of the oldest record found. It will be used as the canonical SURT for this object
     */
    private String oldestSurt;

    /**
     * When was the oldest SURT captured
     */
    private LocalDateTime oldestSurtDate;

    /**
     * Base constructor
     *
     * @param imageURLHash MD5 hash of the image URL
     * @param timestamp image capture timestamp
     * @param url image url
     * @param surt image SURT
     * @param mimeReported image MIME type as recorded by the (W)ARC
     * @param mimeDetected image MIME type as detected by ImageIO
     * @param collection collection to which this image belongs
     * @param bytes image bytes
     * @param warc (W)ARC name
     * @param warcOffset (W)ARC offset in bytes
     */
    public ImageData(String imageURLHash, String timestamp, String url, String surt, String mimeReported, String mimeDetected, String collection, byte[] bytes, String warc, long warcOffset) {
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

        this.warc = warc;
        this.warcOffset = warcOffset;

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

    public long getTimespan() {
        return Duration.between(timestamp.get(0), timestamp.get(timestamp.size()-1)).getSeconds();
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

    public boolean equals(ImageData o) {
        return (o.getContentHash().equals(this.getContentHash()) && o.getSurt().equals(this.getSurt()));
    }

    /**
     * Merges this imageData with this imageData object, taking into account which one is the oldest
     * @param imageData imageData to add
     */
    public void addTimestamp(ImageData imageData) {
        for (LocalDateTime localDT : imageData.timestamp) {
            int compare = localDT.compareTo(timestamp.get(0));
            if (compare < 0 || (compare == 0 && imageData.getUrl().length() < this.getUrl().length()) || (compare == 0 && imageData.getUrl().length() == this.getUrl().length() && imageData.getImageURLHash().compareTo(this.getImageURLHash()) < 0)) {
                imageURLHash = imageData.getImageURLHash();
                contentHash = imageData.getContentHash();
                bytes = imageData.getBytes();
                url = imageData.getUrl();
                surt = imageData.getSurt();
                mimeReported = imageData.getMimeReported();
                mimeDetected = imageData.getMimeDetected();
                collection = imageData.getCollection();
                width = imageData.getWidth();
                height = imageData.getHeight();
                size = imageData.getSize();
                warc = imageData.getWarc();
                warcOffset = imageData.getWarcOffset();
            }
            if (!timestamp.contains(localDT)) {
                this.timestamp.add(localDT);
                this.timestampOriginalFormat.add(getLocalDateTimeToTimestamp(localDT));
                Collections.sort(timestamp);
                Collections.sort(timestampOriginalFormat);
            }
        }
        Collections.sort(timestamp);
        Collections.sort(timestampOriginalFormat);
    }

    public String getId() {
        return timestampOriginalFormat.get(0) + "/" + imageURLHash;
    }

    public String getWarc() {
        return warc;
    }

    public void setWarc(String warc) {
        this.warc = warc;
    }

    public long getWarcOffset() {
        return warcOffset;
    }

    public void setWarcOffset(long warcOffset) {
        this.warcOffset = warcOffset;
    }


    /**
     * Adds required information from matching FullImageMetadata object
     *
     * @param fullImageMetadata FullImageMetadata from which to get the information
     */
    public void assignMetadataToImage(FullImageMetadata fullImageMetadata) {
        this.oldestSurt = fullImageMetadata.getOldestSurt();
        this.oldestSurtDate = fullImageMetadata.getOldestSurtDate();
    }

    public LocalDateTime getOldestSurtDate() {
        return oldestSurtDate;
    }

    public String getOldestSurt() {
        return oldestSurt;
    }
}
