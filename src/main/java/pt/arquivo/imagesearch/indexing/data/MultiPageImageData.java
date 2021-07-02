package pt.arquivo.imagesearch.indexing.data;

import pt.arquivo.imagesearch.indexing.utils.ImageSearchIndexingUtil;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Class used to represent the COMPACT representation of image data objects
 * Instead of having each a separate object for each page an iamge shows up on,
 * we instead use the oldest page as the basis for the obejct and have all relevant metadata
 * in the form List of imgTitle, imgAlt and imgCaption
 */
public class MultiPageImageData implements Comparable<LocalDateTime>, Serializable {

    /**
     * Collection where to which this page matches
     */
    private String collection;

    /**
     * List of image titles for this image
     */
    private List<String> imgTitle;

    /**
     * List of image alt for this image
     */
    private List<String> imgAlt;

    /**
     * List of image captions for this image
     */
    private List<String> imgCaption;


    /**
     * Image crawl timestamp (oldest found)
     */
    private LocalDateTime imgTimestamp;

    /**
     * Image crawl timestamp (newest found)
     */
    private LocalDateTime latestImgTimespan;

    /**
     * Image SHA-256 digest
     */
    private String imageDigest;

    /**
     * Oldest matching page title
     */
    private String pageTitle;

    /**
     * Oldest matching page URL
     */
    private String pageURL;

    /**
     * Oldest matching page url split into word tokens
     */
    private String pageURLTokens;

    /**
     * Deprecated img id form YYYYMMDDHHmmSS/{imageURLHash}
     */
    private String imgId;

    /**
     * Image url
     */
    private String imgURL;

    /**
     * Whether the image is an inline base64 images
     */
    private boolean inline;


    /**
     * Whetehr the image was found in a <a>, <img> or CSS tag
     */
    private Set<String> tagFoundIn;

    /**
     * Oldest matching image url split into word tokens
     */
    private String imgURLTokens;

    /**
     * Image height in pixels
     */
    private int imgHeight;

    /**
     * Image width in pixels
     */
    private int imgWidth;

    /**
     * Image mime type
     */
    private String imgMimeType;

    /**
     * (W)ARC where the image was found
     */
    private String imgWarc;

    /**
     * Offset in the imgWARC in byes
     */
    private long imgWarcOffset;

    /**
     * Oldest page capture timestamp
     */
    private LocalDateTime pageTimestamp;

    /**
     * Oldest page capture timestamp in the YYYYMMDDHHmmSS format
     */
    private String pageTimestampString;

    /**
     * Host of the matching page
     */
    private String pageHost;

    /**
     * Number of images that were matched to this object
     */
    private int matchingImages;

    /**
     * Number of pages that were matched to this object
     */
    private int matchingPages;

    /**
     * Number of times image metadata has changed in this object
     */
    private int imageMetadataChanges;

    /**
     * Number of times page metadata has changed in this object
     */
    private int pageMetadataChanges;

    /**
     * Number of images in the orginal page
     */
    private int imagesInPage;

    /**
     * Combines a regular fullImageMetadata into the COMPACT format
     * @param fullImageMetadata base fullImageMetadata to convert to the MultiPageImageData format
     */
    public MultiPageImageData(FullImageMetadata fullImageMetadata){
        ImageData id = fullImageMetadata.getImageDatas().firstKey();

        this.collection = id.getCollection();

        this.imgTimestamp = id.getTimestamp().get(0);
        this.latestImgTimespan = id.getTimestamp().get(id.getTimestamp().size()-1);
        this.imageDigest = id.getContentHash();

        this.imgId = id.getId();
        this.imgURL = id.getUrl();

        inline = imgURL.startsWith("hash:");
        this.imgURLTokens = "";
        if (!inline) {
            this.imgURLTokens = ImageSearchIndexingUtil.parseURL(id.getUrl());
        }

        this.imgHeight = id.getHeight();
        this.imgWidth = id.getWidth();
        this.imgMimeType = id.getMimeDetected();

        this.imgWarc = id.getWarc();
        this.imgWarcOffset = id.getWarcOffset();


        PageImageData pid = fullImageMetadata.getPageImageDatas().firstKey();

        this.pageTimestamp = pid.getPageTimestamp();
        this.pageTimestampString = pid.getPageTimestampString();
        this.pageTitle = pid.getPageTitle();
        this.pageURL = pid.getPageURL();
        this.pageURLTokens = pid.getPageURLTokens();
        this.pageHost = pid.getPageHost();

        this.imgTitle = new ArrayList<>();
        this.imgAlt = new ArrayList<>();
        this.imgCaption = new ArrayList<>();

        for (PageImageData p: fullImageMetadata.getPageImageDatas().values()){
            if (!p.getImgTitle().isEmpty())
                imgTitle.add(p.getImgTitle());
            if (!p.getImgAlt().isEmpty())
                imgAlt.add(p.getImgAlt());
            if (!p.getImgCaption().isEmpty())
                imgCaption.add(p.getImgCaption());

        }

        matchingImages = pid.getMatchingImages();
        imageMetadataChanges = pid.getImageMetadataChanges();
        pageMetadataChanges = pid.getPageMetadataChanges();
        imagesInPage = pid.getImagesInPage();
        matchingPages = pid.getMatchingPages();
    }

    @Override
    public int compareTo(LocalDateTime timestamp) {
        return this.pageTimestamp.compareTo(timestamp);
    }

    @Override
    public String toString() {
        return String.format("\"%s\": \"%s\", %s", pageTitle, pageURL, pageTimestamp.toString());
    }

    public List<String> getImgTitle() {
        return imgTitle;
    }

    public String getImgURLTokens() {
        return imgURLTokens;
    }

    public String getPageURLTokens() {
        return pageURLTokens;
    }

    public String getImgURL() {
        return imgURL;
    }

    public String getPageTimestampString() {
        return pageTimestampString;
    }

    public String getPageURL() {
        return pageURL;
    }

    public String getPageHost() {
        return pageHost;
    }

    public List<String> getImgCaption() {
        return imgCaption;
    }

    public LocalDateTime getPageTimestamp() {
        return pageTimestamp;
    }

    public String getTimestampsAsStrings() {
        return pageTimestamp.toString();
    }

    public String getPageMetadata() {
        return (String.join(pageTitle.trim(), pageURLTokens).trim()).trim();
    }

    public int getPageMetadataSize() {
        return this.getPageMetadata().length();
    }

    public int getImagesInPage() {
        return imagesInPage;
    }

    public void setImagesInPage(int imagesInPage) {
        this.imagesInPage = imagesInPage;
    }

    public boolean getInline() {
        return inline;
    }

    public Set<String> getTagFoundIn() {
        return tagFoundIn;
    }

    public List<String> getImgAlt() {
        return imgAlt;
    }

    public String getPageTitle() {
        return pageTitle;
    }


    public LocalDateTime getLatastTimestamp() {
        return latestImgTimespan;
    }

    public String getId() {
        return imgId;
    }

    public String getPageURLHash() {
        return ImageSearchIndexingUtil.md5ofString(pageURL);
    }

    public String getImgTimestampString() {
        return imgTimestamp.toString();
    }

    public LocalDateTime getImgTimestamp() {
        return imgTimestamp;
    }

    public void setImgTimestamp(LocalDateTime imgTimestamp) {
        this.imgTimestamp = imgTimestamp;
    }

    public void setImageDigest(String imageDigest) {
        this.imageDigest = imageDigest;
    }

    public String getImageDigest() {
        return imageDigest;
    }

    public int getImgHeight() {
        return imgHeight;
    }

    public int getImgWidth() {
        return imgWidth;
    }

    public void setImgWidth(int imgWidth) {
        this.imgWidth = imgWidth;
    }

    public void setImgHeight(int imgHeight) {
        this.imgHeight = imgHeight;
    }

    public String getImgMimeType() {
        return imgMimeType;
    }

    public void setImgMimeType(String imgMimeType) {
        this.imgMimeType = imgMimeType;
    }

    public String getImgId() {
        return imgId;
    }

    public void setImgId(String imgId) {
        this.imgId = imgId;
    }

    public String getImgWarc() {
        return imgWarc;
    }

    public void setImgWarc(String imgWarc) {
        this.imgWarc = imgWarc;
    }

    public long getImgWarcOffset() {
        return imgWarcOffset;
    }

    public void setImgWarcOffset(long imgWarcOffset) {
        this.imgWarcOffset = imgWarcOffset;
    }

    public String getImgURLHash() {
        return ImageSearchIndexingUtil.md5ofString(imgURL);
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public int getMatchingImages() {
        return matchingImages;
    }

    public int getMatchingPages() {
        return matchingPages;
    }

    public int getImageMetadataChanges() {
        return imageMetadataChanges;
    }

    public int getPageMetadataChanges() {
        return pageMetadataChanges;
    }

    public void setLatestImageTimestamp(LocalDateTime latestImageTimestamp) {
        this.latestImgTimespan = latestImageTimestamp;
    }
}

