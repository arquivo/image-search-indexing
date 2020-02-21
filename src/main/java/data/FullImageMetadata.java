package data;

import java.time.LocalDateTime;
import java.util.Base64;
import java.util.List;

public class FullImageMetadata {

    private String collection;

    //TODO: what to do when more than one image per image url (different timestamps/collections)
    //TODO: what to do when more than one page per image url

    // Info extracted from the image bytes
    private List<LocalDateTime> imgTimestamp;
    private String imgSurt;
    private String imgUrl;

    private String mime;
    private String imgSrcBase64;
    private List<String> imgDigest;

    private int imgWidth;
    private int imgHeight;
    private String imgSrcTokens;

    // Info extracted from the associated page HTML
    private List<LocalDateTime> pageTimestamp;
    private List<String> pageUrl;
    private int pageImages;

    private int imageSize;

    // searchable tokens
    private List<String> pageTitle;
    private List<String> imgTitle;
    private List<String> imgAlt;

    // Externally computed placeholders
    private int safe;
    private int spam;

    // Aggregation metadata
    private int totalMatchingImages;
    private int totalMatchingPages;
    private long imagesInAllMatchingPages;
    private long totalMatchingImgSrc;

    private int totalMetadataChanges;
    private int totalPageMetadataChanges;


    public FullImageMetadata(ImageData image, PageImageData page) {
        this.imgTitle = page.getImgTitle();
        this.imgAlt = page.getImgAlt();
        this.imgSrcTokens = page.getImgSrcTokens();
        this.pageTitle = page.getPageTitle();
        this.pageUrl = page.getPageURL();
        this.imgSurt = image.getSurt();
        this.pageImages = page.getPageImages();

        this.imgUrl = image.getUrl();
        this.mime = image.getMimeDetected();
        this.collection = image.getCollection();
        this.imgDigest = image.getContentHash();
        this.imgSrcBase64 = Base64.getEncoder().encodeToString(image.getBytes());

        this.pageTimestamp = page.getTimestamp();
        this.imgTimestamp = image.getTimestamp();

        this.imgWidth = image.getWidth();
        this.imgHeight = image.getHeight();
        this.imageSize = image.getSize();

        this.safe = -1;
        this.spam = 0;

        this.totalMatchingImages = image.getMatchingImages();
        this.totalMatchingPages = page.getMatchingPages();
        this.imagesInAllMatchingPages = page.getImagesInAllMatchingPages();
        this.totalMatchingImgSrc = page.getTotalMatchingImgReferences();
        this.totalMetadataChanges = Math.max(page.getImgAlt().size(), page.getImgTitle().size());
        this.totalPageMetadataChanges = Math.max(page.getPageURL().size(), page.getPageTitle().size());
    }

    public boolean hasImageMetadata(){
        return !(imgAlt.isEmpty() && imgTitle.isEmpty());
    }
}
