package data;

import java.util.Base64;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class FullImageMetadata {


    public static final int MAXIMUM_META = 50;
    private List<String> imgSurt;
    private List<String> imgUrl;


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
    private List<String> imgSrcTokens;

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

        this.imgSrcTokens = new LinkedList<>();
        this.imgSrcTokens.add(page.getImgSrcTokens());

        this.pageTitle = page.getPageTitle().subList(0, Math.min(page.getPageTitle().size(), MAXIMUM_META));
        this.pageUrl = page.getPageURL().subList(0, Math.min(page.getPageURL().size(), MAXIMUM_META));

        this.imgSurt = new LinkedList<>();
        this.imgSurt.add(image.getSurt());

        this.imgUrl = new LinkedList<>();
        this.imgUrl.add(image.getUrl());

        this.imagesInPage = page.getPageImages();

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

    private List<String> getImgUrl() {
        return imgUrl;
    }

    public List<String> getSurtTokens() {
        return imgSrcTokens;
    }

    public List<String> getImgTitle() {
        return imgTitle;
    }

    public List<String> getImgAlt() {
        return imgAlt;
    }

    public List<String> getImgTimestamp() {
        return imgTimestamp;
    }

    public List<String> getSurt() {
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

    public List<String> getPageTimestamp() {
        return pageTimestamp;
    }

    public List<String> getPageUrl() {
        return pageUrl;
    }

    public List<String> getPageTitle() {
        return pageTitle;
    }

    public boolean hasImageMetadata() {
        return !(imgAlt.isEmpty() && imgTitle.isEmpty());
    }

    public String getImgDigest() {
        return imgDigest.get(0);
    }
}
