package data;

import java.util.*;

public class FullImageMetadata {


    public static final int MAXIMUM_META = 50;

    private Set<String> imgSurt;
    private Set<String> imgUrl;


    // Aggregation metadata
    private int imagesInOriginalPage;
    private int matchingImages;
    private int matchingPages;
    private int imageFilenameChanges;
    private long imagesInAllMatchingPages;
    private long matchingImgReferences;


    private int imageMetadataChanges;
    private int pageMetadataChanges;


    // Info extracted from the image bytes
    private Set<String> imgTimestamp;

    // searchable tokens
    private Set<String> imgTitle;
    private Set<String> imgAlt;
    private Set<String> imgFilenames;
    private Set<String> imgCaption;
    private Set<String> imgSrcTokens;

    private String mime;
    private int imgWidth;
    private int imgHeight;
    private int imageSize;


    // searchable tokens
    private Set<String> pageTitle;
    private Set<String> pageUrl;
    private Set<String> pageHosts;
    // Info extracted from the associated page HTML
    private Set<String> pageTimestamp;

    // Externally computed placeholders
    private int safe;
    private int spam;

    private String imgSrcBase64;
    private Set<String> imgDigest;

    private String collection;


    public FullImageMetadata(ImageData image, PageImageData page) {
        this.imgTitle = new HashSet<>();
        this.imgTitle.addAll(page.getImgTitles().subList(0, Math.min(page.getImgTitles().size(), MAXIMUM_META)));

        this.imgAlt = new HashSet<>();
        this.imgAlt.addAll(page.getImgAlts().subList(0, Math.min(page.getImgAlts().size(), MAXIMUM_META)));

        this.imgSrcTokens = new HashSet<>();
        if (!page.getImgSrcTokens().isEmpty())
            this.imgSrcTokens.add(page.getImgSrcTokens());

        this.pageTitle = new HashSet<>();
        this.pageTitle.addAll(page.getPageTitles().subList(0, Math.min(page.getPageTitles().size(), MAXIMUM_META)));

        this.pageUrl = new HashSet<>();
        this.pageUrl.addAll(page.getPageURLs().subList(0, Math.min(page.getPageURLs().size(), MAXIMUM_META)));

        this.pageHosts = new HashSet<>();
        this.pageHosts.addAll(page.getPageHosts().subList(0, Math.min(page.getPageHosts().size(), MAXIMUM_META)));

        this.imgSurt = new HashSet<>();
        this.imgSurt.add(image.getSurt());

        this.imgUrl = new HashSet<>();
        this.imgUrl.add(image.getUrl());

        this.imgFilenames = new HashSet<>();
        this.imgFilenames.addAll(page.getImgFilenames());

        this.imgCaption = new HashSet<>();
        this.imgCaption.addAll(page.getImgCaptions());

        this.imagesInOriginalPage = page.getPageImages();

        this.mime = image.getMimeDetected();
        this.collection = image.getCollection();

        this.imgDigest = new HashSet<>();
        this.imgDigest.addAll(image.getContentHash());

        this.imgSrcBase64 = Base64.getEncoder().encodeToString(image.getBytes());

        this.pageTimestamp = new HashSet<>();
        this.pageTimestamp.addAll(page.getTimestampsAsStrings().subList(0, Math.min(page.getTimestampsAsStrings().size(), MAXIMUM_META)));

        this.imgTimestamp = new HashSet<>();
        this.imgTimestamp.addAll(image.getTimestampsAsStrings().subList(0, Math.min(image.getTimestampsAsStrings().size(), MAXIMUM_META)));

        this.imgWidth = image.getWidth();
        this.imgHeight = image.getHeight();
        this.imageSize = image.getSize();

        this.safe = -1;
        this.spam = 0;

        this.matchingImages = image.getMatchingImages();
        this.matchingPages = page.getMatchingPages();
        this.imagesInAllMatchingPages = page.getImagesInAllMatchingPages();
        this.imagesInOriginalPage = page.getImagesInOriginalPage();
        this.matchingImgReferences = page.getTotalMatchingImgReferences();
        this.imageMetadataChanges = page.getImageMetadataChanges();
        this.imageFilenameChanges = page.getImageFilenameChanges();
        this.pageMetadataChanges = Math.max(page.getPageURLs().size(), page.getPageTitles().size());
    }


    public void merge(FullImageMetadata result) {
        this.imgTitle.addAll(result.getImgTitle());
        this.imgAlt.addAll(result.getImgAlt());

        this.imgUrl.addAll(result.getImgUrl());

        this.pageTitle.addAll(result.getPageTitle());
        this.pageUrl.addAll(result.getPageUrl());
        this.imgSurt.addAll(result.getSurt());
        this.imgSrcTokens.addAll(result.getSurtTokens());

        this.pageTimestamp.addAll(result.getPageTimestamp());
        this.imgTimestamp.addAll(result.getImgTimestamp());

        this.pageHosts.addAll(result.getPageHosts());


        this.imagesInOriginalPage = Math.max(this.imagesInOriginalPage, result.getImagesInOriginalPage());

        this.matchingImages += result.getMatchingImages();

        this.matchingPages += result.getMatchingPages();
        this.imagesInAllMatchingPages += result.getImagesInAllMatchingPages();
        this.matchingImgReferences += result.getMatchingImgReferences();
        this.imageMetadataChanges += result.getImageMetadataChanges();
        this.pageMetadataChanges += result.getPageMetadataChanges();
    }

    private Set<String> getPageHosts() {
        return pageHosts;
    }

    private Set<String> getImgUrl() {
        return imgUrl;
    }

    public Set<String> getSurtTokens() {
        return imgSrcTokens;
    }

    public Set<String> getImgTitle() {
        return imgTitle;
    }

    public Set<String> getImgAlt() {
        return imgAlt;
    }

    public Set<String> getImgTimestamp() {
        return imgTimestamp;
    }

    public Set<String> getSurt() {
        return imgSurt;
    }

    public int getImagesInOriginalPage() {
        return imagesInOriginalPage;
    }

    public int getMatchingImages() {
        return matchingImages;
    }

    public int getMatchingPages() {
        return matchingPages;
    }

    public long getImagesInAllMatchingPages() {
        return imagesInAllMatchingPages;
    }

    public long getMatchingImgReferences() {
        return matchingImgReferences;
    }

    public int getImageMetadataChanges() {
        return imageMetadataChanges;
    }

    public int getPageMetadataChanges() {
        return pageMetadataChanges;
    }

    public Set<String> getPageTimestamp() {
        return pageTimestamp;
    }

    public Set<String> getPageUrl() {
        return pageUrl;
    }

    public Set<String> getPageTitle() {
        return pageTitle;
    }

    public boolean hasImageMetadata() {
        return !(imgAlt.isEmpty() && imgTitle.isEmpty() && imgCaption.isEmpty());
    }

    public Set<String> getImgDigests() {
        return imgDigest;
    }
}
