package data;


import utils.WARCInformationParser;

import java.time.LocalDateTime;

public class PageImageData implements Comparable<LocalDateTime> {

    private String type;
    private String imgTitle;
    private String imgAlt;
    private String imgSrcTokens;

    private String pageTitle;
    private String pageURLTokens;


    private String imgSrc;
    private String imageSurt;

    private int pageImages;
    private String pageTstamp;

    private String pageURL;
    private String pageHost;
    private String pageProtocol;

    private LocalDateTime timestamp;

    public PageImageData(String type, String imgTitle, String imgAlt, String imgSrcTokens, String pageTitle, String pageURLTokens, String imgSrc, String imageSurt, int pageImages, String pageTstamp, String pageURL, String pageHost, String pageProtocol) {
        this.type = type;
        this.imgTitle = imgTitle;
        this.imgAlt = imgAlt;
        this.imgSrcTokens = imgSrcTokens;
        this.pageTitle = pageTitle;
        this.pageURLTokens = pageURLTokens;
        this.imgSrc = imgSrc;
        this.imageSurt = imageSurt;
        this.pageImages = pageImages;
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
        return pageImages;
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
}
