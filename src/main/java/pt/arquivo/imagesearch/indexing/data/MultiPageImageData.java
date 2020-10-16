package pt.arquivo.imagesearch.indexing.data;

import pt.arquivo.imagesearch.indexing.utils.ImageSearchIndexingUtil;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class MultiPageImageData implements Comparable<LocalDateTime>, Serializable {

    private String collection;

    private String type;
    private List<String> imgTitle;
    private List<String> imgAlt;
    private List<String> imgCaption;
    private LocalDateTime imgTimestamp;
    private LocalDateTime latestImgTimespan;
    private String imageDigest;

    private String pageTitle;
    private String pageURL;
    private String pageURLTokens;

    private String imgId;
    private String imgURL;

    private boolean inline;
    private Set<String> tagFoundIn;

    private String imgURLTokens;
    private int imgHeight;
    private int imgWidth;
    private String imgMimeType;

    private String imgWarc;
    private long imgWarcOffset;

    private LocalDateTime pageTimestamp;
    private String pageTimestampString;

    private String pageHost;

    private int matchingImages;
    private int matchingPages;

    private int imageMetadataChanges;
    private int pageMetadataChanges;
    private int imagesInPage;

    public MultiPageImageData(FullImageMetadata fullImageMetadata){
        ImageData id = fullImageMetadata.getImageDatas().firstKey();

        this.collection = id.getCollection();

        this.type = "mixed";
        this.imgTimestamp = id.getTimestamp().get(0);
        this.latestImgTimespan = id.getTimestamp().get(id.getTimestamp().size()-1);
        this.imageDigest = id.getContentHash();

        this.imgId = id.getId();
        this.imgURL = id.getUrl();

        inline = imgURL.startsWith("hash:");
        this.imgURLTokens = "";
        if (!inline) {
            this.imgURLTokens = ImageSearchIndexingUtil.parseURL(id.getUrl());
        }

        this.imgHeight = id.getHeight();
        this.imgWidth = id.getWidth();
        this.imgMimeType = id.getMimeDetected();

        this.imgWarc = id.getWarc();
        this.imgWarcOffset = id.getWarcOffset();


        PageImageData pid = fullImageMetadata.getPageImageDatas().firstKey();

        this.pageTimestamp = pid.getPageTimestamp();
        this.pageTimestampString = pid.getPageTimestampString();
        this.pageTitle = pid.getPageTitle();
        this.pageURL = pid.getPageURL();
        this.pageURLTokens = pid.getPageURLTokens();
        this.pageHost = pid.getPageHost();

        this.imgTitle = new ArrayList<>();
        this.imgAlt = new ArrayList<>();
        this.imgCaption = new ArrayList<>();

        for (PageImageData p: fullImageMetadata.getPageImageDatas().values()){
            if (!p.getImgTitle().isEmpty())
                imgTitle.add(p.getImgTitle());
            if (!p.getImgAlt().isEmpty())
                imgAlt.add(p.getImgAlt());
            if (!p.getImgCaption().isEmpty())
                imgCaption.add(p.getImgCaption());

        }

        matchingImages = pid.getMatchingImages();
        imageMetadataChanges = pid.getImageMetadataChanges();
        pageMetadataChanges = pid.getPageMetadataChanges();
        imagesInPage = pid.getImagesInPage();
        matchingPages = pid.getMatchingPages();
    }

    @Override
    public int compareTo(LocalDateTime timestamp) {
        return this.pageTimestamp.compareTo(timestamp);
    }

    @Override
    public String toString() {
        return String.format("\"%s\": \"%s\", %s", pageTitle, pageURL, pageTimestamp.toString());
    }

    public String getType() {
        return type;
    }

    public List<String> getImgTitle() {
        return imgTitle;
    }

    public String getImgURLTokens() {
        return imgURLTokens;
    }

    public String getPageURLTokens() {
        return pageURLTokens;
    }

    public String getImgURL() {
        return imgURL;
    }

    public String getPageTimestampString() {
        return pageTimestampString;
    }

    public String getPageURL() {
        return pageURL;
    }

    public String getPageHost() {
        return pageHost;
    }

    public List<String> getImgCaption() {
        return imgCaption;
    }

    public LocalDateTime getPageTimestamp() {
        return pageTimestamp;
    }

    public String getTimestampsAsStrings() {
        return pageTimestamp.toString();
    }

    public String getPageMetadata() {
        return (String.join(pageTitle.trim(), pageURLTokens).trim()).trim();
    }

    public int getPageMetadataSize() {
        return this.getPageMetadata().length();
    }

    public int getImagesInPage() {
        return imagesInPage;
    }

    public void setImagesInPage(int imagesInPage) {
        this.imagesInPage = imagesInPage;
    }

    public boolean getInline() {
        return inline;
    }

    public Set<String> getTagFoundIn() {
        return tagFoundIn;
    }

    public List<String> getImgAlt() {
        return imgAlt;
    }

    public String getPageTitle() {
        return pageTitle;
    }


    public LocalDateTime getLatastTimestamp() {
        return latestImgTimespan;
    }

    public String getId() {
        return imgId;
    }

    public String getPageURLHash() {
        return ImageSearchIndexingUtil.md5ofString(pageURL);
    }

    public String getImgTimestampString() {
        return imgTimestamp.toString();
    }

    public LocalDateTime getImgTimestamp() {
        return imgTimestamp;
    }

    public void setImgTimestamp(LocalDateTime imgTimestamp) {
        this.imgTimestamp = imgTimestamp;
    }

    public void setImageDigest(String imageDigest) {
        this.imageDigest = imageDigest;
    }

    public String getImageDigest() {
        return imageDigest;
    }

    public int getImgHeight() {
        return imgHeight;
    }

    public int getImgWidth() {
        return imgWidth;
    }

    public void setImgWidth(int imgWidth) {
        this.imgWidth = imgWidth;
    }

    public void setImgHeight(int imgHeight) {
        this.imgHeight = imgHeight;
    }

    public void assignImageToPage(ImageData id, LocalDateTime correct) {
        this.setImgTimestamp(correct);
        this.setLatestImageTimestamp(id.getTimestamp().get(id.getTimestamp().size()-1));
        this.setImageDigest(id.getContentHash());
        this.setImgHeight(id.getHeight());
        this.setImgWidth(id.getWidth());
        this.setImgMimeType(id.getMimeDetected());
        this.setImgId(id.getId());
        this.setImgWarc(id.getWarc());
        this.setImgWarcOffset(id.getWarcOffset());
    }

    public String getImgMimeType() {
        return imgMimeType;
    }

    public void setImgMimeType(String imgMimeType) {
        this.imgMimeType = imgMimeType;
    }

    public String getImgId() {
        return imgId;
    }

    public void setImgId(String imgId) {
        this.imgId = imgId;
    }

    public String getImgWarc() {
        return imgWarc;
    }

    public void setImgWarc(String imgWarc) {
        this.imgWarc = imgWarc;
    }

    public long getImgWarcOffset() {
        return imgWarcOffset;
    }

    public void setImgWarcOffset(long imgWarcOffset) {
        this.imgWarcOffset = imgWarcOffset;
    }

    public String getImgURLHash() {
        return ImageSearchIndexingUtil.md5ofString(imgURL);
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public int getMatchingImages() {
        return matchingImages;
    }

    public int getMatchingPages() {
        return matchingPages;
    }

    public int getImageMetadataChanges() {
        return imageMetadataChanges;
    }

    public int getPageMetadataChanges() {
        return pageMetadataChanges;
    }

    public void setLatestImageTimestamp(LocalDateTime latestImageTimestamp) {
        this.latestImgTimespan = latestImageTimestamp;
    }
}

