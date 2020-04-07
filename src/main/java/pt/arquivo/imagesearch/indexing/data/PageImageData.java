package pt.arquivo.imagesearch.indexing.data;


import org.apache.commons.io.FilenameUtils;

import java.net.MalformedURLException;
import java.net.URL;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import pt.arquivo.imagesearch.indexing.utils.WARCInformationParser;

import pt.arquivo.imagesearch.indexing.utils.ImageSearchIndexingUtil;

public class PageImageData implements Comparable<LocalDateTime> {

    private static final int MAX_ADD_THRESHOLD = 50;
    private String type;
    private List<String> imgTitles;
    private List<String> imgAlts;
    private List<String> imgFilenames;
    private List<String> imgCaptions;

    private List<String> pageTitles;
    private List<String> pageURLTokens;

    private String imgSrc;
    private String imgSrcTokens;
    private String imgSurt;

    private List<String> pageTimestamps;
    private List<String> pageURLs;

    private List<String> pageHosts;
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
    private int imageMetadataChanges;

    // Number of times the metadata changed for the image
    private int imageFilenameChanges;

    private boolean isInline;

    private Set<String> tagFoundIn;


    public PageImageData(String type, String imgTitles, String imgAlts, String imgSrcTokens, String imgCaption, String pageTitles, String pageURLTokens, String imgSrc, String imageSurt, int imagesInOriginalPage, int imagesInAllMatchingPages, int totalMatchingImgReferences, String pageTimestamps, String pageURLs, String pageHost, String pageProtocol, String tagType) {
        this.type = type;

        this.imgAlts = new LinkedList<>();
        imgAlts = imgAlts.trim();
        if (!imgAlts.isEmpty())
            this.imgAlts.add(imgAlts);

        this.tagFoundIn = new HashSet<>();

        tagFoundIn.add(tagType);

        isInline = imgSrc.startsWith("hash:");

        this.imgSrcTokens = "";


        this.imgFilenames = new LinkedList<>();

        if (!isInline) {
            this.imgSrcTokens = imgSrcTokens;
            URL url = null;
            try {
                url = new URL(imgSrc);
                String filename = FilenameUtils.getBaseName(url.getPath());
                if (!filename.isEmpty())
                    this.imgFilenames.add(ImageSearchIndexingUtil.cleanPunctuation(filename));
            } catch (MalformedURLException ignored) {

            }
        }

        this.imgCaptions = new LinkedList<>();
        imgCaption = imgCaption.trim();
        if (!imgCaption.isEmpty())
            this.imgCaptions.add(imgCaption);

        this.pageTitles = new LinkedList<>();
        pageTitles = pageTitles.trim();
        if (!pageTitles.isEmpty())
            this.pageTitles.add(pageTitles);


        this.imgTitles = new LinkedList<>();
        imgTitles = imgTitles.trim();
        if (!imgTitles.isEmpty())
            this.imgTitles.add(imgTitles);


        this.pageURLTokens = new LinkedList<>();
        if (!pageURLTokens.trim().isEmpty())
            this.pageURLTokens.add(pageURLTokens);

        this.imgSrc = imgSrc;
        this.imgSurt = imageSurt;

        this.imagesInOriginalPage = imagesInOriginalPage;
        this.imagesInAllMatchingPages = imagesInAllMatchingPages;

        this.totalMatchingImgReferences = totalMatchingImgReferences;
        this.matchingPages = 0;

        this.imageMetadataChanges = 0;
        this.imageFilenameChanges = 0;

        this.pageTimestamps = new LinkedList<>();
        this.pageTimestamps.add(pageTimestamps);

        this.pageURLs = new LinkedList<>();
        if (!pageURLs.trim().isEmpty())
            this.pageURLs.add(pageURLs);

        this.pageHosts = new LinkedList<>();
        if (!pageHost.trim().isEmpty())
            this.pageHosts.add(pageHost);

        this.pageProtocol = pageProtocol;

        this.timestamp = new LinkedList<>();
        this.timestamp.add(WARCInformationParser.parseLocalDateTime(pageTimestamps));
    }

    @Override
    public int compareTo(LocalDateTime timestamp) {
        return this.timestamp.get(0).compareTo(timestamp);
    }

    @Override
    public String toString() {
        return String.format("\"%s\": \"%s\", %s", String.join(";", pageTitles), String.join(";", pageURLs), String.join(";", timestamp.toString()));
    }

    public String getType() {
        return type;
    }

    public List<String> getImgTitles() {
        return imgTitles;
    }

    public List<String> getImgAlts() {
        return imgAlts;
    }

    public String getImgSrcTokens() {
        return imgSrcTokens;
    }

    public List<String> getPageTitles() {
        return pageTitles;
    }

    public List<String> getPageURLTokens() {
        return pageURLTokens;
    }

    public String getImgSrc() {
        return imgSrc;
    }

    public String getImgSurt() {
        return imgSurt;
    }

    public int getPageImages() {
        return imagesInOriginalPage;
    }

    public List<String> getPageTimestamps() {
        return pageTimestamps;
    }

    public List<String> getPageURLs() {
        return pageURLs;
    }

    public List<String> getImgFilenames() {
        return imgFilenames;
    }

    public List<String> getPageHosts() {
        return pageHosts;
    }

    public List<String> getImgCaptions() {
        return imgCaptions;
    }

    private void addPageURLTokens(List<String> pageURLTokens) {
        for (String tokens : pageURLTokens)
            if (this.pageURLTokens.size() <= MAX_ADD_THRESHOLD && !tokens.isEmpty() && !this.pageURLTokens.contains(tokens))
                this.pageURLTokens.add(tokens);
    }

    private void addPageTitle(List<String> pageTitle) {
        for (String title : pageTitle){
            title = title.trim();
            if (this.pageTitles.size() <= MAX_ADD_THRESHOLD && !pageTitle.isEmpty() && !this.pageTitles.contains(title))
                this.pageTitles.add(title);
        }
    }
    public void addPageURL(List<String> pageURL) {
        for (String url : pageURL) {
            url = url.trim();
            if (this.pageURLs.size() <= MAX_ADD_THRESHOLD && !url.isEmpty() && !this.pageURLs.contains(url))
                this.pageURLs.add(url);
        }
    }

    public void addPageHosts(List<String> pageHosts) {
        for (String host : pageHosts) {
            host = host.trim();
            if (this.pageHosts.size() <= MAX_ADD_THRESHOLD && !host.isEmpty() && !this.pageHosts.contains(host))
                this.pageHosts.add(host);
        }
    }


    public void addImgTitle(List<String> imgTitle) {
        for (String title : imgTitle){
            title = title.trim();
            if (!title.isEmpty() && !this.imgTitles.contains(title)) {
                //TODO: this check is not correct, as not all information is kept
                //There is a MAX_ADD_THRESHOLD, thus, if the same metadata shows up twice after the length of
                //the threshold is exceed, the counter will be incremented twice
                imageMetadataChanges++;
                if (this.imgTitles.size() <= MAX_ADD_THRESHOLD)
                    this.imgTitles.add(title);
            }
        }
    }

    public void addImgAlt(List<String> imgAlt) {
        for (String alt : imgAlt){
            alt = alt.trim();
            if (!alt.isEmpty() && !this.imgAlts.contains(alt)) {
                imageMetadataChanges++;
                if (this.imgAlts.size() <= MAX_ADD_THRESHOLD)
                    this.imgAlts.add(alt);
            }
        }
    }

    public void addImgFilenames(List<String> imgFilenames) {
        for (String filename : imgFilenames){
            filename = filename.trim();
            if (!filename.isEmpty() && !this.imgFilenames.contains(filename)) {
                imageFilenameChanges++;
                if (this.imgFilenames.size() <= MAX_ADD_THRESHOLD)
                    this.imgFilenames.add(filename);
            }
        }
    }

    public void addImgCaptions(List<String> imgCaptions) {
        for (String caption : imgCaptions){
            caption = caption.trim();
            if (!caption.isEmpty() && !this.imgCaptions.contains(caption)) {
                imageMetadataChanges++;
                if (this.imgCaptions.size() <= MAX_ADD_THRESHOLD)
                    this.imgCaptions.add(caption);
            }
        }
    }

    public void addPageTimestamps(List<String> pageTstamps) {
        for (String timestamp : pageTstamps)
            if (this.pageTimestamps.size() <= MAX_ADD_THRESHOLD && !this.pageTimestamps.contains(timestamp))
                this.pageTimestamps.add(timestamp);
    }

    public void addImgTimestamps(List<LocalDateTime> imgTimestamps) {
        for (LocalDateTime timestamp : imgTimestamps)
            if (this.timestamp.size() <= MAX_ADD_THRESHOLD && !this.timestamp.contains(timestamp))
                this.timestamp.add(timestamp);
    }


    public String getPageProtocol() {
        return pageProtocol;
    }

    public List<LocalDateTime> getTimestamp() {
        return timestamp;
    }

    public List<String> getTimestampsAsStrings() {
        List<String> results = new LinkedList<>();
        for (LocalDateTime time : this.timestamp)
            results.add(time.toString());
        return results;
    }

    /*
    public String getPageMetadata() {
        Set<String> pt.arquivo.imagesearch.indexing.data = new HashSet<>();
        pt.arquivo.imagesearch.indexing.data.addAll(Arrays.asList(pageTitle.split(" ")));
        pt.arquivo.imagesearch.indexing.data.addAll(Arrays.asList(pageURLTokens.split(" ")));
        return String.join(" ", pt.arquivo.imagesearch.indexing.data.toArray(new String[0]));
    }

    public String getImageMetadata() {
        Set<String> pt.arquivo.imagesearch.indexing.data = new HashSet<>();
        pt.arquivo.imagesearch.indexing.data.addAll(Arrays.asList(imgTitle.split(" ")));
        pt.arquivo.imagesearch.indexing.data.addAll(Arrays.asList(imgAlt.split(" ")));
        return String.join(" ", pt.arquivo.imagesearch.indexing.data.toArray(new String[0]));
    }
     */

    public String getPageMetadata() {
        return (String.join(" ", pageTitles).trim() + " " + String.join(" ", pageURLTokens).trim()).trim();
    }

    public String getImageMetadata() {
        return (String.join(" ", imgTitles).trim() + " " + String.join(" ", imgAlts).trim()).trim();
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

    public int getImageMetadataChanges() {
        return imageMetadataChanges;
    }

    public int getImageFilenameChanges() {
        return imageFilenameChanges;
    }

    public void incrementMatchingPages(int matchingPages) {
        this.matchingPages += matchingPages;
    }

    public boolean isInline() {
        return isInline;
    }

    public Set<String> getTagFoundIn() {
        return tagFoundIn;
    }

    public boolean addPageImageData(PageImageData newPageImageData) {

        int initalSize = this.imgAlts.size() + this.imgTitles.size();

        this.addImgAlt(newPageImageData.getImgAlts());
        this.addImgTitle(newPageImageData.getImgTitles());

        this.addPageTitle(newPageImageData.getPageTitles());
        this.addPageURL(newPageImageData.getPageURLs());
        this.addPageURLTokens(newPageImageData.getPageURLTokens());

        this.addImgFilenames(newPageImageData.getImgFilenames());

        this.addPageTimestamps(newPageImageData.getPageTimestamps());
        this.addImgTimestamps(newPageImageData.getTimestamp());

        this.addImgCaptions(newPageImageData.getImgCaptions());

        this.tagFoundIn.addAll(newPageImageData.getTagFoundIn());

        this.imagesInOriginalPage = Math.max(this.imagesInOriginalPage, newPageImageData.getImagesInOriginalPage());
        this.imagesInAllMatchingPages += newPageImageData.getImagesInAllMatchingPages();

        this.totalMatchingImgReferences += newPageImageData.getTotalMatchingImgReferences();

        //TODO: this may need to be changed, as it will increment if an image is found twice in the same page
        this.matchingPages += newPageImageData.getMatchingPages();

        this.imageMetadataChanges += newPageImageData.getImageMetadataChanges();

        this.imageFilenameChanges += newPageImageData.getImageFilenameChanges();

        int finalSize = this.imgAlts.size() + this.imgTitles.size();

        return initalSize != finalSize;

    }
}

