package data;


import utils.WARCInformationParser;

import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;

public class PageImageData implements Comparable<LocalDateTime> {

    private String type;
    private List<String> imgTitle;
    private List<String> imgAlt;

    private List<String> pageTitle;
    private List<String> pageURLTokens;

    private String imgSrc;
    private String imgSrcTokens;
    private String imageSurt;

    private List<String> pageTstamp;

    private List<String> pageURL;
    private String pageHost;
    private String pageProtocol;

    private List<LocalDateTime> timestamp;

    // Number of images in the original page
    private int imagesInOriginalPage;

    // Number of images in all referenced pages
    private long imagesInAllMatchingPages;

    // Total number of matching <img src="">
    private int totalMatchingImgReferences;

    // Number of pages referencing the image
    private int matchingPages;

    // Number of times the metadata changed for the image
    private int metadataChanges;


    public PageImageData(String type, String imgTitle, String imgAlt, String imgSrcTokens, String pageTitle, String pageURLTokens, String imgSrc, String imageSurt, int imagesInOriginalPage, int imagesInAllMatchingPages, int totalMatchingImgReferences, String pageTstamp, String pageURL, String pageHost, String pageProtocol) {
        this.type = type;

        this.imgAlt = new LinkedList<>();
        if (!imgAlt.trim().isEmpty())
            this.imgAlt.add(imgAlt);

        this.imgSrcTokens = imgSrcTokens;

        this.pageTitle = new LinkedList<>();
        if (!pageTitle.trim().isEmpty())
            this.pageTitle.add(pageTitle);


        this.imgTitle = new LinkedList<>();
        if (!imgTitle.trim().isEmpty())
            this.imgTitle.add(imgTitle);


        this.pageURLTokens = new LinkedList<>();
        if (!pageURLTokens.trim().isEmpty())
            this.pageURLTokens.add(pageURLTokens);

        this.imgSrc = imgSrc;
        this.imageSurt = imageSurt;

        this.imagesInOriginalPage = imagesInOriginalPage;
        this.imagesInAllMatchingPages = imagesInAllMatchingPages;

        this.totalMatchingImgReferences = totalMatchingImgReferences;
        this.matchingPages = 0;


        this.pageTstamp = new LinkedList<>();
        this.pageTstamp.add(pageTstamp);

        this.pageURL = new LinkedList<>();
        if (!pageURL.trim().isEmpty())
            this.pageURL.add(pageURL);

        this.pageHost = pageHost;
        this.pageProtocol = pageProtocol;

        this.timestamp = new LinkedList<>();
        this.timestamp.add(WARCInformationParser.parseLocalDateTime(pageTstamp));
    }

    private void addPageURLTokens(List<String> pageURLTokens) {
        for (String tokens : pageURLTokens)
            if (!tokens.isEmpty() && !this.pageURLTokens.contains(tokens))
                this.pageURLTokens.add(tokens);
    }

    private void addPageTitle(List<String> pageTitle) {
        for (String title : pageTitle)
            if (!pageTitle.isEmpty() && !this.pageTitle.contains(title))
                this.pageTitle.add(title);
    }

    @Override
    public int compareTo(LocalDateTime timestamp) {
        return this.timestamp.get(0).compareTo(timestamp);
    }

    @Override
    public String toString() {
        return String.format("\"%s\": \"%s\", %s", String.join(";", pageTitle), String.join(";", pageURL), String.join(";", timestamp.toString()));
    }

    public String getType() {
        return type;
    }

    public List<String> getImgTitle() {
        return imgTitle;
    }

    public List<String> getImgAlt() {
        return imgAlt;
    }

    public String getImgSrcTokens() {
        return imgSrcTokens;
    }

    public List<String> getPageTitle() {
        return pageTitle;
    }

    public List<String> getPageURLTokens() {
        return pageURLTokens;
    }

    public String getImgSrc() {
        return imgSrc;
    }

    public String getImageSurt() {
        return imageSurt;
    }

    public int getPageImages() {
        return imagesInOriginalPage;
    }

    public List<String> getPageTstamp() {
        return pageTstamp;
    }

    public List<String> getPageURL() {
        return pageURL;
    }

    public String getPageHost() {
        return pageHost;
    }

    public void addPageURL(List<String> pageURL) {
        for (String url : pageURL)
            if (!url.isEmpty() && !this.pageURL.contains(url))
                this.pageURL.add(url);
    }

    public void addImgTitle(List<String> imgTitle) {
        for (String title : imgTitle)
            if (!title.trim().isEmpty() && !this.imgTitle.contains(title))
                this.imgTitle.add(title);
    }

    public void addImgAlt(List<String> imgAlt) {
        for (String alt : imgAlt)
            if (!alt.trim().isEmpty() && !this.imgAlt.contains(alt))
                this.imgAlt.add(alt);
    }

    public String getPageProtocol() {
        return pageProtocol;
    }

    public List<LocalDateTime> getTimestamp() {
        return timestamp;
    }

    public List<String> getTimestampsAsStrings() {
        List<String> results = new LinkedList<>();
        for (LocalDateTime time: this.timestamp)
            results.add(time.toString());
        return results;
    }

    /*
    public String getPageMetadata() {
        Set<String> data = new HashSet<>();
        data.addAll(Arrays.asList(pageTitle.split(" ")));
        data.addAll(Arrays.asList(pageURLTokens.split(" ")));
        return String.join(" ", data.toArray(new String[0]));
    }

    public String getImageMetadata() {
        Set<String> data = new HashSet<>();
        data.addAll(Arrays.asList(imgTitle.split(" ")));
        data.addAll(Arrays.asList(imgAlt.split(" ")));
        return String.join(" ", data.toArray(new String[0]));
    }
     */

    public String getPageMetadata() {
        return (String.join(" ", pageTitle).trim() + " " + String.join(" ", pageURLTokens).trim()).trim();
    }

    public String getImageMetadata() {
        return (String.join(" ", imgTitle).trim() + " " + String.join(" ", imgAlt).trim()).trim();
    }

    public int getPageMetadataSize() {
        return this.getPageMetadata().length();
    }

    public int getImageMetadataSize() {
        return this.getImageMetadata().length();
    }

    public int getImagesInOriginalPage() {
        return imagesInOriginalPage;
    }

    public void setImagesInOriginalPage(int imagesInOriginalPage) {
        this.imagesInOriginalPage = imagesInOriginalPage;
    }

    public long getImagesInAllMatchingPages() {
        return imagesInAllMatchingPages;
    }

    public void setImagesInAllMatchingPages(long imagesInAllMatchingPages) {
        this.imagesInAllMatchingPages = imagesInAllMatchingPages;
    }

    public void incrementImagesInAllMatchingPages(long imagesInAllMatchingPages) {
        this.imagesInAllMatchingPages += imagesInAllMatchingPages;
    }

    public int getTotalMatchingImgReferences() {
        return totalMatchingImgReferences;
    }

    public void setTotalMatchingImgReferences(int totalMatchingImgReferences) {
        this.totalMatchingImgReferences = totalMatchingImgReferences;
    }

    public void incrementMatchingImageReferences(int totalMatchingImgReferences) {
        this.totalMatchingImgReferences += totalMatchingImgReferences;
    }

    public int getMatchingPages() {
        return matchingPages;
    }

    public void setMatchingPages(int matchingPages) {
        this.matchingPages = matchingPages;
    }

    public void incrementMatchingPages(int matchingPages) {
        this.matchingPages += matchingPages;
    }

    public int getMetadataChanges() {
        return metadataChanges;
    }

    public void incrementMetadataChanges(int metadataChanges) {
        this.metadataChanges += metadataChanges;
    }

    public boolean addPageImageData(PageImageData newPageImageData) {

        int initalSize = this.imgAlt.size() + this.imgTitle.size();

        this.addImgAlt(newPageImageData.getImgAlt());
        this.addImgTitle(newPageImageData.getImgTitle());

        this.addPageTitle(newPageImageData.getPageTitle());
        this.addPageURL(newPageImageData.getPageURL());
        this.addPageURLTokens(newPageImageData.getPageURLTokens());

        for(String timestamp: newPageImageData.getPageTstamp())
            if (!this.pageTstamp.contains(timestamp))
                this.pageTstamp.add(timestamp);

        for(LocalDateTime timestamp: newPageImageData.getTimestamp())
            if (!this.timestamp.contains(timestamp))
                this.timestamp.add(timestamp);

        this.imagesInOriginalPage += newPageImageData.getImagesInOriginalPage();
        this.imagesInAllMatchingPages += newPageImageData.getImagesInAllMatchingPages();

        this.totalMatchingImgReferences += newPageImageData.getTotalMatchingImgReferences();
        this.matchingPages += newPageImageData.getMatchingPages();

        int finalSize = this.imgAlt.size() + this.imgTitle.size();

        return initalSize != finalSize;

    }
}

