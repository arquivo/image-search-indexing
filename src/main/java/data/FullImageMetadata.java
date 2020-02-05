package data;

public class FullImageMetadata {
    private String imageHashKey;
    private String tstamp;
    private String url;

    private String surt;
    private String mime;
    private String collection;
    private String contentHash;
    private String bytes64String;
    private String imgTitle;
    private String imgAlt;
    private String imgSrcTokens;
    private String pageTitle;
    private String pageURLTokens;
    private String imgSrc;

    private String imageSurt;
    private int pageImages;
    private int safe;
    private int spam;
    private String pageTstamp;
    private String pageURL;
    private String pageHost;
    private String pageProtocol;

    public FullImageMetadata(ImageData image, PageImageData page) {
        this.imgTitle = page.getImgTitle();
        this.imgAlt = page.getImgAlt();
        this.imgSrcTokens = page.getImgSrcTokens();
        this.pageTitle = page.getPageTitle();
        this.pageURLTokens = page.getPageURLTokens();
        this.imgSrc = page.getImgSrc();
        this.imageSurt = page.getImageSurt();
        this.pageImages = page.getPageImages();
        //this.pageTstamp = page.getPageTstamp();
        this.pageURL = page.getPageURL();
        this.pageHost = page.getPageHost();
        this.pageProtocol = page.getPageProtocol();

        this.imageHashKey = image.getImageHashKey();
        this.tstamp = image.getTstamp();
        this.url = image.getUrl();
        this.surt = image.getSurt();
        this.mime = image.getMime();
        this.collection = image.getCollection();
        this.contentHash = image.getContentHash();
        this.bytes64String = image.getBytes64String();

        this.pageTstamp = page.getTimestamp().toString();
        this.tstamp = image.getTimestamp().toString();

        this.safe = -1;
        this.spam = 0;

    }

}
