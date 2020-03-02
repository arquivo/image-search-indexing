package data;

import java.util.*;

public class FullImageMetadata {


    public static final int MAXIMUM_META = 50;
    private Set<String> imgSurt;
    private Set<String> imgUrl;


    // Aggregation metadata
    private int totalMatchingImages;
    private int totalMatchingPages;
    private int imagesInPage;
    private long imagesInAllPages;
    private long totalImgSrc;

    private int totalImgMetadataChanges;
    private int totalPageMetadataChanges;


    // Info extracted from the image bytes
    private Set<String> imgTimestamp;

    // searchable tokens
    private Set<String> imgTitle;
    private Set<String> imgAlt;
    private Set<String> imgSrcTokens;

    private String mime;
    private int imgWidth;
    private int imgHeight;
    private int imageSize;


    // searchable tokens
    private Set<String> pageTitle;
    private Set<String> pageUrl;
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
        this.imgTitle.addAll(page.getImgTitle().subList(0, Math.min(page.getImgTitle().size(), MAXIMUM_META)));

        this.imgAlt = new HashSet<>();
        this.imgAlt.addAll(page.getImgAlt().subList(0, Math.min(page.getImgAlt().size(), MAXIMUM_META)));

        this.imgSrcTokens = new HashSet<>();
        if (!page.getImgSrcTokens().isEmpty())
            this.imgSrcTokens.add(page.getImgSrcTokens());

        this.pageTitle = new HashSet<>();
        this.pageTitle.addAll(page.getPageTitle().subList(0, Math.min(page.getPageTitle().size(), MAXIMUM_META)));

        this.pageUrl = new HashSet<>();
        this.pageUrl.addAll(page.getPageURL().subList(0, Math.min(page.getPageURL().size(), MAXIMUM_META)));

        this.imgSurt = new HashSet<>();
        this.imgSurt.add(image.getSurt());

        this.imgUrl = new HashSet<>();
        this.imgUrl.add(image.getUrl());

        this.imagesInPage = page.getPageImages();

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

        this.totalMatchingImages = image.getMatchingImages();
        this.totalMatchingPages = page.getMatchingPages();
        this.imagesInAllPages = page.getImagesInAllMatchingPages();
        this.totalImgSrc = page.getTotalMatchingImgReferences();
        this.totalImgMetadataChanges = page.getImageMetadataChanges();
        this.totalPageMetadataChanges = Math.max(page.getPageURL().size(), page.getPageTitle().size());
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

        this.imagesInPage += result.getPageImages();

        this.totalMatchingImages += result.getMatchingImages();
        this.totalMatchingPages += result.getMatchingPages();
        this.imagesInAllPages += result.getImagesInAllMatchingPages();
        this.totalImgSrc += result.getTotalMatchingImgReferences();
        this.totalImgMetadataChanges += result.getImageMetadataChanges();
        this.totalPageMetadataChanges += result.getTotalPageMetadataChanges();
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


    public int getPageImages() {
        return imagesInPage;
    }

    public int getMatchingImages() {
        return totalMatchingImages;
    }

    public int getMatchingPages() {
        return totalMatchingPages;
    }

    public long getImagesInAllMatchingPages() {
        return imagesInAllPages;
    }

    public long getTotalMatchingImgReferences() {
        return totalImgSrc;
    }

    public int getImageMetadataChanges() {
        return totalImgMetadataChanges;
    }

    public int getTotalPageMetadataChanges() {
        return totalPageMetadataChanges;
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
        return !(imgAlt.isEmpty() && imgTitle.isEmpty());
    }

    public String getImgDigest() {
        return (String) imgDigest.toArray()[0];
    }
}
