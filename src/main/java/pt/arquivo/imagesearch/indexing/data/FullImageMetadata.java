package pt.arquivo.imagesearch.indexing.data;

import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import pt.arquivo.imagesearch.indexing.DupDigestMergerJob;
import pt.arquivo.imagesearch.indexing.data.comparators.ImageDataComparator;
import pt.arquivo.imagesearch.indexing.data.comparators.PageImageDataComparator;

import java.awt.*;
import java.io.*;
import java.time.LocalDateTime;
import java.util.*;

public class FullImageMetadata implements Writable, Serializable {

    private transient Logger logger = Logger.getLogger(FullImageMetadata.class);

    public static final int MAXIMUM_META = 50;

    private TreeMap<ImageData, ImageData> imageDatas;
    private TreeMap<PageImageData, PageImageData> pageImageDatas;

    private int matchingImages;
    private int urlChanges;

    // Aggregation metadata
    private int matchingPages;
    private int imagesInPages;
    private long matchingImgReferences;
    private int imageFilenameChanges;

    private int imageMetadataChanges;
    private int pageMetadataChanges;

    public FullImageMetadata() {
        this.imageDatas = new TreeMap<>(new ImageDataComparator());
        this.pageImageDatas = new TreeMap<>(new PageImageDataComparator());
    }

    public FullImageMetadata(FullImageMetadata metadata) {
        imageDatas = (TreeMap<ImageData, ImageData>) metadata.getImageDatas().clone();
        pageImageDatas = (TreeMap<PageImageData, PageImageData>) metadata.getPageImageDatas().clone();

        matchingImages = metadata.getMatchingImages();
        urlChanges = metadata.getUrlChanges();

        matchingPages = metadata.getMatchingPages();
        imagesInPages = metadata.getImagesInPages();
        matchingImgReferences = metadata.getMatchingImgReferences();
        imageFilenameChanges = metadata.getImageFilenameChanges();

        imageMetadataChanges = metadata.getImageMetadataChanges();
        pageMetadataChanges = metadata.getPageMetadataChanges();
    }

    public FullImageMetadata(FullImageMetadata metadata, ImageData imageData) {

        imageDatas = new TreeMap<>(new ImageDataComparator());
        imageDatas.put(imageData, imageData);

        pageImageDatas = new TreeMap<>(new PageImageDataComparator());
        for (PageImageData pageImageData: metadata.pageImageDatas.values()){
            if (imageData.getTimestampOriginalFormat().contains(pageImageData.getImgTimestamp())){
                pageImageDatas.put(pageImageData, pageImageData);
            }
        }

        matchingImages = metadata.getMatchingImages();
        urlChanges = metadata.getUrlChanges();

        matchingPages = metadata.getMatchingPages();
        imagesInPages = metadata.getImagesInPages();
        matchingImgReferences = metadata.getMatchingImgReferences();
        imageFilenameChanges = metadata.getImageFilenameChanges();

        imageMetadataChanges = metadata.getImageMetadataChanges();
        pageMetadataChanges = metadata.getPageMetadataChanges();
    }

    public void merge(FullImageMetadata result) {

        int counter = 0;
        for (ImageData data : result.getImageDatasValues()){
            this.addImageData(data);
            counter++;
            if (counter >= 10000) {
                logger.info(String.format("Broke iterating: %d/%d image records", counter, result.getImageDatasValues().size()));
                break;
            }

        }

        counter = 0;
        for (PageImageData data : result.getPageImageDatasValues()){
            this.addPageImageData(data);
            counter++;
            if (counter >= 10000) {
                logger.info(String.format("Broke iterating: %d/%d image records", counter, result.getPageImageDatasValues().size()));
                break;
            }
        }

    }

    public boolean addImageData(ImageData imageData) {
        this.matchingImages++;

        ImageData id = imageDatas.get(imageData);
        if (id != null) {
            id.addTimestamp(imageData);
            imageDatas.put(imageData, id);
            return false;
        } else {
            imageDatas.put(imageData, imageData);
            return true;
        }
    }

    public boolean addPageImageData(PageImageData pageImageData) {
        this.matchingPages++;

        this.matchingImgReferences += pageImageData.getImgReferencesInPage();
        this.imagesInPages += pageImageData.getImagesInPage();

        if (pageImageDatas.get(pageImageData) != null) {
            pageImageDatas.get(pageImageData).updatePageTimestamp(pageImageData);
            PageImageData updated = pageImageDatas.get(pageImageData);
            // add entry in case image changed
            pageImageDatas.put(pageImageData, updated);
            return false;
        } else {
            this.imageMetadataChanges++;
            this.pageMetadataChanges++;
            pageImageDatas.put(pageImageData, pageImageData);
            return true;
        }
    }

    public Collection<ImageData> getImageDatasValues() {
        return imageDatas.values();
    }

    public TreeMap<ImageData, ImageData> getImageDatas() {
        return imageDatas;
    }

    public Collection<PageImageData> getPageImageDatasValues() {
        return pageImageDatas.values();
    }

    public TreeMap<PageImageData, PageImageData> getPageImageDatas() {
        return pageImageDatas;
    }

    public boolean hasImageMetadata() {
        for (PageImageData pid : pageImageDatas.values())
            if (!pid.getImageMetadata().isEmpty() || !pid.getPageMetadata().isEmpty())
                return true;
        return false;
    }

    public void assignImagesToPages() {
        if (imageDatas.isEmpty())
            return;

        TreeMap<LocalDateTime, ImageData> map = new TreeMap<>();
        for (ImageData data : this.getImageDatasValues())
            for (LocalDateTime timestamp : data.getTimestamp())
                map.put(timestamp, data);

        for (PageImageData data : this.getPageImageDatasValues()) {
            LocalDateTime timestamp = data.getPageTimestamp();

            LocalDateTime before = map.floorKey(timestamp);
            LocalDateTime after = map.ceilingKey(timestamp);
            LocalDateTime correct;
            if (before == null) correct = after;
            else if (after == null) correct = before;
            else if ((after.compareTo(timestamp)) < (timestamp.compareTo(before))) correct = after;
            else correct = before;
            ImageData id = map.get(correct);
            data.setImgTimestamp(correct);
            data.setImageDigest(id.getContentHash());
        }
    }


    public int getMatchingImages() {
        return matchingImages;
    }

    public int getUrlChanges() {
        return urlChanges;
    }

    public int getMatchingPages() {
        return matchingPages;
    }

    public int getImagesInPages() {
        return imagesInPages;
    }

    public long getMatchingImgReferences() {
        return matchingImgReferences;
    }

    public int getImageFilenameChanges() {
        return imageFilenameChanges;
    }

    public int getImageMetadataChanges() {
        return imageMetadataChanges;
    }

    public int getPageMetadataChanges() {
        return pageMetadataChanges;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(this);
        byte[] data = baos.toByteArray();
        int size = data.length;
        dataOutput.writeInt(size);
        dataOutput.write(data);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int size = dataInput.readInt();
        byte[] data = new byte[size];
        dataInput.readFully(data);
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        ObjectInputStream ois = new ObjectInputStream(bais);
        try {
            FullImageMetadata other = (FullImageMetadata) ois.readObject();

            this.imageDatas = other.getImageDatas();
            this.pageImageDatas = other.getPageImageDatas();

            this.matchingImages = other.getMatchingImages();
            this.urlChanges = other.getUrlChanges();
            this.matchingPages = other.getMatchingPages();
            this.imagesInPages = other.getImagesInPages();
            this.matchingImgReferences = other.getMatchingImgReferences();
            this.imageFilenameChanges = other.getImageFilenameChanges();
            this.imageMetadataChanges = other.getImageMetadataChanges();
            this.pageMetadataChanges = other.getPageMetadataChanges();

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    public String toString() {
        //private TreeMap<ImageData, ImageData> imageDatas;
        //private TreeMap<PageImageData, PageImageData> pageImageDatas;
        String result = "";
        for (ImageData i : imageDatas.values())
            for (String t : i.getTimestampOriginalFormat())
                result += t + " ";
        result += "## ";
        for (PageImageData i : pageImageDatas.values())
            result += i.getPageTimestamp() + " ";
        return result;
    }
}
