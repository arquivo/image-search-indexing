package data;

import java.util.Base64;
import java.util.List;

public class FullImageMetadata {


    public static final int MAXIMUM_META = 50;
    private String imgSurt;
    private String imgUrl;


    // Aggregation metadata
    private int totalMatchingImages;
    private int totalMatchingPages;
    private int imagesInPage;
    private long imagesInAllPages;
    private long totalImgSrc;

    private int totalImgMetadataChanges;
    private int totalPageMetadataChanges;



    // Info extracted from the image bytes
    private List<String> imgTimestamp;

    // searchable tokens
    private List<String> imgTitle;
    private List<String> imgAlt;
    private String imgSrcTokens;

    private String mime;
    private int imgWidth;
    private int imgHeight;
    private int imageSize;


    // searchable tokens
    private List<String> pageTitle;
    private List<String> pageUrl;
    // Info extracted from the associated page HTML
    private List<String> pageTimestamp;

    // Externally computed placeholders
    private int safe;
    private int spam;

    private String imgSrcBase64;
    private List<String> imgDigest;

    private String collection;


    public FullImageMetadata(ImageData image, PageImageData page) {
        this.imgTitle = page.getImgTitle().subList(0, Math.min(page.getImgTitle().size(), MAXIMUM_META));
        this.imgAlt = page.getImgAlt().subList(0, Math.min(page.getImgAlt().size(), MAXIMUM_META));
        this.imgSrcTokens = page.getImgSrcTokens();
        this.pageTitle = page.getPageTitle().subList(0, Math.min(page.getPageTitle().size(), MAXIMUM_META));
        this.pageUrl = page.getPageURL().subList(0, Math.min(page.getPageURL().size(), MAXIMUM_META));
        this.imgSurt = image.getSurt();
        this.imagesInPage = page.getPageImages();

        this.imgUrl = image.getUrl();
        this.mime = image.getMimeDetected();
        this.collection = image.getCollection();
        this.imgDigest = image.getContentHash();
        this.imgSrcBase64 = Base64.getEncoder().encodeToString(image.getBytes());

        this.pageTimestamp = page.getTimestampsAsStrings().subList(0, Math.min(page.getTimestampsAsStrings().size(), MAXIMUM_META));
        this.imgTimestamp = image.getTimestampsAsStrings().subList(0, Math.min(image.getTimestampsAsStrings().size(), MAXIMUM_META));

        this.imgWidth = image.getWidth();
        this.imgHeight = image.getHeight();
        this.imageSize = image.getSize();

        this.safe = -1;
        this.spam = 0;

        this.totalMatchingImages = image.getMatchingImages();
        this.totalMatchingPages = page.getMatchingPages();
        this.imagesInAllPages = page.getImagesInAllMatchingPages();
        this.totalImgSrc = page.getTotalMatchingImgReferences();
        this.totalImgMetadataChanges = page.getImageMetadataChanges();
        this.totalPageMetadataChanges = Math.max(page.getPageURL().size(), page.getPageTitle().size());
    }

    public boolean hasImageMetadata(){
        return !(imgAlt.isEmpty() && imgTitle.isEmpty());
    }
}
