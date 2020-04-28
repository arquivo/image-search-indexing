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

    private String type;
    private String imgTitle;
    private String imgAlt;
    private String imgFilename;
    private String imgCaption;
    private String imgTimestampString;

    private String pageTitle;
    private String pageURLTokens;

    private String imgURL;
    private String imgURLTokens;
    private String imgSurt;

    private LocalDateTime pageTimestamp;
    private String pageTimestampString;
    private String pageURL;
    private String pageURLHash;

    private String pageHost;
    private String pageProtocol;



    // Number of images in the original page
    private int imagesInPage;

    // Total number of matching <img src="">
    private int imgReferencesInPage;

    private boolean inline;

    private Set<String> tagFoundIn;
    private String imageDigest;

    public PageImageData(String type, String imgTitle, String imgAlt, String imgURLTokens, String imgCaption, String pageTitle, String pageURLTokens, String imgURL, String imageSurt, int imagesInPage, int imgReferencesInPage, String pageTimestampString, String pageURL, String pageHost, String pageProtocol, String tagType) {
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

        this.pageTimestampString = pageTimestampString;

        this.pageURL = pageURL;
        this.pageURLHash = ImageSearchIndexingUtil.md5ofString(pageURL);

        this.pageHost = pageHost;

        this.pageProtocol = pageProtocol;

        this.pageTimestamp = WARCInformationParser.parseLocalDateTime(pageTimestampString);
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
        if (pageImageData.getPageTimestamp().compareTo(this.pageTimestamp) < 0) {
            pageTimestamp = pageImageData.getPageTimestamp();
            pageTimestampString = WARCInformationParser.getLocalDateTimeToTimestamp(pageTimestamp);
        }
    }

    public String getId() {
        return pageTimestampString + "/" + pageURLHash;
    }

    public String getPageURLHash() {
        return pageURLHash;
    }

    public String getImgTimestamp() {
        return imgTimestampString;
    }

    public void setImgTimestamp(String imgTimestampString) {
        this.imgTimestampString = imgTimestampString;
    }

    public void setImgTimestamp(LocalDateTime timestamp) {
        this.imgTimestampString = WARCInformationParser.getLocalDateTimeToTimestamp(timestamp);
    }

    public void setImageDigest(String imageDigest) {
        this.imageDigest = imageDigest;
    }

    public String getImageDigest() {
        return imageDigest;
    }
}

