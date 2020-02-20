package data;


import utils.WARCInformationParser;

import java.time.LocalDateTime;
import java.util.*;

public class PageImageData implements Comparable<LocalDateTime> {

    private String type;
    private String imgTitle;
    private String imgAlt;
    private String imgSrcTokens;

    private String pageTitle;
    private String pageURLTokens;


    private String imgSrc;
    private String imageSurt;

    // Number of images in the original page
    private int imagesInOriginalPage;

    // Number of images in the original page
    private long imagesInAllMatchingPages;

    // Number of images in the original page
    private int matchingPages;


    private String pageTstamp;

    private String pageURL;
    private String pageHost;
    private String pageProtocol;

    private LocalDateTime timestamp;

    public PageImageData(String type, String imgTitle, String imgAlt, String imgSrcTokens, String pageTitle, String pageURLTokens, String imgSrc, String imageSurt, int imagesInOriginalPage, int imagesInAllMatchingPages, int matchingPages, String pageTstamp, String pageURL, String pageHost, String pageProtocol) {
        this.type = type;
        this.imgTitle = imgTitle;
        this.imgAlt = imgAlt;
        this.imgSrcTokens = imgSrcTokens;
        this.pageTitle = pageTitle;
        this.pageURLTokens = pageURLTokens;
        this.imgSrc = imgSrc;
        this.imageSurt = imageSurt;

        this.imagesInOriginalPage = imagesInOriginalPage;
        this.imagesInAllMatchingPages = imagesInAllMatchingPages;

        this.matchingPages = matchingPages;

        this.pageTstamp = pageTstamp;
        this.pageURL = pageURL;
        this.pageHost = pageHost;
        this.pageProtocol = pageProtocol;

        this.timestamp = WARCInformationParser.parseLocalDateTime(pageTstamp);

    }

    @Override
    public int compareTo(LocalDateTime timestamp) {
        return this.timestamp.compareTo(timestamp);
    }

    @Override
    public String toString() {
        return String.format("\"%s\": \"%s\", %s", pageTitle, pageURL, timestamp.toString());
    }

    public String getType() {
        return type;
    }

    public String getImgTitle() {
        return imgTitle;
    }

    public String getImgAlt() {
        return imgAlt;
    }

    public String getImgSrcTokens() {
        return imgSrcTokens;
    }

    public String getPageTitle() {
        return pageTitle;
    }

    public String getPageURLTokens() {
        return pageURLTokens;
    }

    public String getImgSrc() {
        return imgSrc;
    }

    public String getImageSurt() {
        return imageSurt;
    }

    public int getPageImages() {
        return imagesInOriginalPage;
    }

    public String getPageTstamp() {
        return pageTstamp;
    }

    public String getPageURL() {
        return pageURL;
    }

    public String getPageHost() {
        return pageHost;
    }

    public String getPageProtocol() {
        return pageProtocol;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public String getPageMetadata() {
        Set<String> data = new HashSet<>();
        data.addAll(Arrays.asList(pageTitle.split(" ")));
        data.addAll(Arrays.asList(pageURLTokens.split(" ")));
        return String.join(" ", data.toArray(new String[0]));
    }

    public String getImageMetadata() {
        Set<String> data = new HashSet<>();
        data.addAll(Arrays.asList(imgTitle.split(" ")));
        data.addAll(Arrays.asList(imgAlt.split(" ")));
        return String.join(" ", data.toArray(new String[0]));
    }

    public int getPageMetadataSize() {
        return this.getPageMetadata().length();
    }

    public int getImageMetadataSize() {
        return this.getImageMetadata().length();
    }

    public int getImagesInOriginalPage() {
        return imagesInOriginalPage;
    }

    public void setImagesInOriginalPage(int imagesInOriginalPage) {
        this.imagesInOriginalPage = imagesInOriginalPage;
    }

    public long getImagesInAllMatchingPages() {
        return imagesInAllMatchingPages;
    }

    public void setImagesInAllMatchingPages(long imagesInAllMatchingPages) {
        this.imagesInAllMatchingPages = imagesInAllMatchingPages;
    }

    public int getMatchingPages() {
        return matchingPages;
    }

    public void setMatchingPages(int matchingPages) {
        this.matchingPages = matchingPages;
    }

    public void incrementImagesInAllMatchingPages(long imagesInAllMatchingPages) {
        this.imagesInAllMatchingPages += imagesInAllMatchingPages;
    }

    public void incrementMatchingPages(int matchingPages) {
        this.matchingPages += matchingPages;
    }
}

