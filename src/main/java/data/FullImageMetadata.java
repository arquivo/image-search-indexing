package data;

import java.util.Base64;

public class FullImageMetadata {

    private String collection;

    //TODO: what to do when more than one image per image url (different timestamps/collections)
    //TODO: what to do when more than one page per image url

    // Info extracted from the image bytes
    private String imgTimestamp;
    private String imgSurt;
    private String imgUrl;

    private String mime;
    private String imgSrcBase64;
    private String imgDigest;

    private int imgWidth;
    private int imgHeight;
    private String imgSrcTokens;

    // Info extracted from the associated page HTML
    private String pageTimestamp;
    private String pageUrl;
    private String pageHost;
    private String pageProtocol;
    private int pageImages;

    private int imageSize;

    // searchable tokens
    private String pageTitle;
    private String pageURLTokens;
    private String imgTitle;
    private String imgAlt;

    // Externally computed placeholders
    private int safe;
    private int spam;

    // Aggregation metadata
    private int totalMatchingImages;
    private int totalMatchingPages;
    private long imagesInAllMatchingPages;
    private long totalMatchingImgSrc;

    private int totalMetadataChanges;


    public FullImageMetadata(ImageData image, PageImageData page, int totalMatchingImages, int totalMatchingPages, long totalMatchingImgSrc, long imagesInAllMatchingPages, int totalMetadataChanges) {
        this.imgTitle = page.getImgTitle();
        this.imgAlt = page.getImgAlt();
        this.imgSrcTokens = page.getImgSrcTokens();
        this.pageTitle = page.getPageTitle();
        this.pageURLTokens = page.getPageURLTokens();
        this.imgSurt = image.getSurt();
        this.pageImages = page.getPageImages();
        this.pageUrl = page.getPageURL();
        this.pageHost = page.getPageHost();
        this.pageProtocol = page.getPageProtocol();

        this.imgUrl = image.getUrl();
        this.mime = image.getMimeDetected();
        this.collection = image.getCollection();
        this.imgDigest = image.getContentHash();
        this.imgSrcBase64 = Base64.getEncoder().encodeToString(image.getBytes());

        this.pageTimestamp = page.getTimestamp().toString();
        this.imgTimestamp = image.getTimestamp().toString();

        this.imgWidth = image.getWidth();
        this.imgHeight = image.getHeight();
        this.imageSize = image.getSize();

        this.safe = -1;
        this.spam = 0;

        this.totalMatchingImages = totalMatchingImages;
        this.totalMatchingPages = totalMatchingPages;
        this.imagesInAllMatchingPages = imagesInAllMatchingPages;
        this.totalMatchingImgSrc = totalMatchingImgSrc;
        this.totalMetadataChanges = totalMetadataChanges;
    }
}
