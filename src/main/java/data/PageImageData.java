package data;

public class PageImageData {
    String imgTitle;
    String imgAlt;
    String imgSrcTokens;

    String pageTitle;
    String pageURLTokens;


    String imgSrc;
    String imageSurt;

    int pageImages;
    String pageTstamp;

    String pageURL;
    String pageHost;
    String pageProtocol;

    public PageImageData(String imgTitle, String imgAlt, String imgSrcTokens, String pageTitle, String pageURLTokens, String imgSrc, String imageSurt, int pageImages, String pageTstamp, String pageURL, String pageHost, String pageProtocol) {
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
    }
}
