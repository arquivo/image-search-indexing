package pt.arquivo.imagesearch.indexing.data;


import org.apache.commons.io.FilenameUtils;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;

import pt.arquivo.imagesearch.indexing.utils.WARCInformationParser;

import pt.arquivo.imagesearch.indexing.utils.ImageSearchIndexingUtil;

public class PageImageData implements Comparable<LocalDateTime>, Serializable {

    private static final int MAX_ADD_THRESHOLD = 50;

    private String collection;

    private String warc;
    private long warcOffset;

    private String type;
    private String imgTitle;
    private String imgAlt;
    private String imgFilename;
    private String imgCaption;
    private LocalDateTime imgTimestamp;

    private String pageTitle;
    private String pageURLTokens;

    private String imgId;
    private String imgURL;
    private String imgURLTokens;
    private String imgSurt;
    private int imgHeight;
    private int imgWidth;
    private String imgMimeType;

    private String imgWarc;
    private long imgWarcOffset;

    private LocalDateTime pageTimestamp;
    private String pageTimestampString;
    private String pageURL;

    private String pageHost;
    private String pageProtocol;



    // Number of images in the original page
    private int imagesInPage;

    // Total number of matching <img src="">
    private int imgReferencesInPage;

    private boolean inline;

    private Set<String> tagFoundIn;
    private String imageDigest;

    public PageImageData(String type, String imgTitle, String imgAlt, String imgURLTokens, String imgCaption, String pageTitle, String pageURLTokens, String imgURL, String imageSurt, int imagesInPage, int imgReferencesInPage, String pageTimestampString, String pageURL, String pageHost, String pageProtocol, String tagType, String warc, long warcOffset, String collection) {

        this.type = type;

        this.imgAlt = imgAlt;

        this.tagFoundIn = new HashSet<>();

        tagFoundIn.add(tagType);

        inline = imgURL.startsWith("hash:");

        this.imgURLTokens = "";

        if (!inline) {
            this.imgURLTokens = imgURLTokens;
            URL url = null;
            try {
                url = new URL(imgURL);
                String filename = FilenameUtils.getBaseName(url.getPath());
                if (!filename.isEmpty())
                    this.imgFilename = ImageSearchIndexingUtil.cleanPunctuation(filename);
            } catch (MalformedURLException ignored) {

            }
        }

        this.imgCaption = imgCaption;

        this.pageTitle = pageTitle;

        this.imgTitle = imgTitle;

        this.pageURLTokens = pageURLTokens;

        this.imgURL = imgURL;
        this.imgSurt = imageSurt;

        this.imagesInPage = imagesInPage;
        this.imgReferencesInPage = imgReferencesInPage;

        this.pageURL = pageURL;

        this.pageHost = pageHost;

        this.pageProtocol = pageProtocol;

        this.pageTimestampString = pageTimestampString;
        this.pageTimestamp = WARCInformationParser.parseLocalDateTime(pageTimestampString);

        this.warc = warc;
        this.warcOffset = warcOffset;

        this.collection = collection;
    }

    @Override
    public int compareTo(LocalDateTime timestamp) {
        return this.pageTimestamp.compareTo(timestamp);
    }

    @Override
    public String toString() {
        return String.format("\"%s\": \"%s\", %s", pageTitle, pageURL, pageTimestamp.toString());
    }

    public String getType() {
        return type;
    }

    public String getImgTitle() {
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

    public String getImgSurt() {
        return imgSurt;
    }

    public String getPageTimestampString() {
        return pageTimestampString;
    }

    public String getPageURL() {
        return pageURL;
    }

    public String getImgFilename() {
        return imgFilename;
    }

    public String getPageHost() {
        return pageHost;
    }

    public String getImgCaption() {
        return imgCaption;
    }

    public String getPageProtocol() {
        return pageProtocol;
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

    public String getImageMetadata() {
        return (String.join(" ", imgTitle.trim(), imgAlt.trim(), imgCaption.trim()).trim()).trim();
    }

    public int getPageMetadataSize() {
        return this.getPageMetadata().length();
    }

    public int getImageMetadataSize() {
        return this.getImageMetadata().length();
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

    public String getImgAlt() {
        return imgAlt;
    }

    public String getPageTitle() {
        return pageTitle;
    }

    public int getImgReferencesInPage() {
        return imgReferencesInPage;
    }

    public boolean equals(PageImageData o) {
        return (o.getImageMetadata().equals(this.getImageMetadata()));
    }

    public void incrementImgReferencesInPage(int i) {
        imgReferencesInPage += i;
    }

    public void updatePageTimestamp(PageImageData pageImageData) {
        int comparison = pageImageData.getPageTimestamp().compareTo(this.pageTimestamp);
        // if two pages with the same metadata are parsed, store the oldest one
        // if two pages with the same timestamp and metadata are parsed, store the one with the shortest URL
        // useful for donated collections
        if (comparison < 0 || (comparison == 0 && pageImageData.getPageURL().length() < pageURL.length())) {
            pageTimestamp = pageImageData.getPageTimestamp();
            pageTimestampString = WARCInformationParser.getLocalDateTimeToTimestamp(pageTimestamp);
            pageURL = pageImageData.getPageURL();
            pageURLTokens = pageImageData.getPageURLTokens();
            pageTitle = pageImageData.getPageTitle();
            pageHost = pageImageData.getPageHost();
            pageProtocol = pageImageData.getPageProtocol();
            imagesInPage = pageImageData.getImagesInPage();
            imgReferencesInPage = pageImageData.getImgReferencesInPage();
        }
    }

    public String getId() {
        return pageTimestampString + "/" + ImageSearchIndexingUtil.md5ofString(pageURL) + "/" + ImageSearchIndexingUtil.md5ofString(getImageMetadata()) + "/" + imageDigest;
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

    public void assignImageToPage(ImageData id, LocalDateTime correct) {
        this.setImgTimestamp(correct);
        this.setImageDigest(id.getContentHash());
        this.setImgHeight(id.getHeight());
        this.setImgWidth(id.getWidth());
        this.setImgMimeType(id.getMimeDetected());
        this.setImgId(id.getId());
        this.setImgWarc(id.getWarc());
        this.setImgWarcOffset(id.getWarcOffset());
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
}

