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

    public static final int MAXIMUM_META = 500;

    private TreeMap<ImageData, ImageData> imageDatas;
    private TreeMap<PageImageData, PageImageData> pageImageDatas;

    private String oldestSurt;
    private LocalDateTime oldestSurtDate;

    private int uniqueDigestsOnURL;

    private int matchingImages;
    private int matchingPages;

    private int imageMetadataChanges;
    private int pageMetadataChanges;

    public FullImageMetadata() {
        this.imageDatas = new TreeMap<>(new ImageDataComparator());
        this.pageImageDatas = new TreeMap<>(new PageImageDataComparator());
        this.imageMetadataChanges = 0;
        this.pageMetadataChanges = 1;
        this.matchingImages = 0;
        this.matchingPages = 0;
        this.uniqueDigestsOnURL = 0;
    }

    public FullImageMetadata(FullImageMetadata metadata) {
        imageDatas = (TreeMap<ImageData, ImageData>) metadata.getImageDatas().clone();
        pageImageDatas = (TreeMap<PageImageData, PageImageData>) metadata.getPageImageDatas().clone();

        imageMetadataChanges = metadata.getImageMetadataChanges();
        pageMetadataChanges = metadata.getPageMetadataChanges();

        matchingImages = metadata.getMatchingImages();
        matchingPages = metadata.getMatchingPages();

        uniqueDigestsOnURL = metadata.getUniqueDigestsOnURL();

        oldestSurt = metadata.getOldestSurt();
        oldestSurtDate = metadata.getOldestSurtDate();
    }

    public FullImageMetadata(FullImageMetadata metadata, ImageData imageData) {

        imageDatas = new TreeMap<>(new ImageDataComparator());
        imageDatas.put(imageData, imageData);

        pageImageDatas = new TreeMap<>(new PageImageDataComparator());
        for (PageImageData pageImageData: metadata.pageImageDatas.values()){
            if (imageData.getTimestamp().contains(pageImageData.getImgTimestamp())){
                pageImageDatas.put(pageImageData, pageImageData);
            }
        }

        uniqueDigestsOnURL = metadata.getUniqueDigestsOnURL();

        matchingImages = metadata.getMatchingImages();
        matchingPages = metadata.getMatchingPages();

        imageMetadataChanges = metadata.getImageMetadataChanges();
        pageMetadataChanges = metadata.getPageMetadataChanges();

        oldestSurt = metadata.getOldestSurt();
        oldestSurtDate = metadata.getOldestSurtDate();
    }

    public void merge(FullImageMetadata metadata) {
        int matchingImagesOriginal = this.matchingImages;
        int matchingPagesOriginal = this.matchingPages;

        for (ImageData data : metadata.getImageDatasValues()){
            if (this.imageDatas.size() < MAXIMUM_META) {
                this.addImageData(data);
            } else {
                break;
            }
        }

        for (PageImageData data : metadata.getPageImageDatasValues()){
            if (this.pageImageDatas.size() < MAXIMUM_META) {
                this.addPageImageData(data);
            } else {
                break;
            }
        }
        //This line avoids double counting the newly added pages and images
        //and enables counting images that were not parsec due to the MAXIMUM_META limit
        this.matchingImages = matchingImagesOriginal + metadata.getMatchingImages();
        this.matchingPages = matchingPagesOriginal + metadata.getMatchingPages();

        if (metadata.getOldestSurtDate() == null)
            return;

        if (oldestSurtDate == null){
            oldestSurt = metadata.getOldestSurt();
            oldestSurtDate = metadata.getOldestSurtDate();
        } else {
            int comparator = metadata.getOldestSurtDate().compareTo(oldestSurtDate);
            if (comparator < 0 || (comparator == 0 && oldestSurt.length() < metadata.getOldestSurt().length()) || (comparator == 0 && oldestSurt.length() == metadata.getOldestSurt().length() && metadata.getOldestSurt().compareTo(oldestSurt) < 0)) {
                oldestSurt = metadata.getOldestSurt();
                oldestSurtDate = metadata.getOldestSurtDate();
            }
        }
    }

    public boolean addImageData(ImageData imageData) {
        this.matchingImages++;

        if (oldestSurtDate == null){
            oldestSurt = imageData.getSurt();
            oldestSurtDate = imageData.getTimestamp().get(0);
        } else {
            int comparator = imageData.getTimestamp().get(0).compareTo(oldestSurtDate);
            if (comparator < 0 || (comparator == 0 && oldestSurt.length() < imageData.getSurt().length()) || (comparator == 0 && oldestSurt.length() == imageData.getSurt().length() && imageData.getSurt().compareTo(oldestSurt) < 0)) {
                oldestSurt = imageData.getSurt();
                oldestSurtDate = imageData.getTimestamp().get(0);
            }
        }

        ImageData id = imageDatas.get(imageData);
        if (id != null) {
            id.addTimestamp(imageData);
            imageDatas.put(imageData, id);
            return false;
        } else {
            this.uniqueDigestsOnURL += 1;
            imageDatas.put(imageData, imageData);
            return true;
        }
    }

    public boolean addPageImageData(PageImageData newPageImageData) {
        this.matchingPages++;

        PageImageData pageImageData = pageImageDatas.get(newPageImageData);
        if (pageImageData != null) {
            if (!pageImageData.getPageMetadata().equals(newPageImageData.getPageMetadata()))
                this.pageMetadataChanges++;

            pageImageData.updatePageTimestamp(newPageImageData);
            // add entry in case image changed
            pageImageDatas.put(newPageImageData, pageImageData);
            return false;
        } else {
            this.imageMetadataChanges++;
            pageImageDatas.put(newPageImageData, newPageImageData);
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
        for (ImageData data : this.getImageDatasValues()) {
            data.assignMetadataToImage(this);
            for (LocalDateTime timestamp : data.getTimestamp())
                map.put(timestamp, data);
        }

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
            data.assignImageToPage(id, correct);
            data.assignMetadataToPage(this);
        }
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

    public int getUniqueDigestsOnURL() {
        return uniqueDigestsOnURL;
    }

    public void setUniqueDigestsOnURL(int uniqueDigestsOnURL) {
        this.uniqueDigestsOnURL = uniqueDigestsOnURL;
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
            this.matchingPages = other.getMatchingPages();
            this.imageMetadataChanges = other.getImageMetadataChanges();
            this.pageMetadataChanges = other.getPageMetadataChanges();

            this.uniqueDigestsOnURL = other.getUniqueDigestsOnURL();

            this.oldestSurt = other.getOldestSurt();
            this.oldestSurtDate = other.getOldestSurtDate();

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    public String getOldestSurt() {
        return oldestSurt;
    }

    public LocalDateTime getOldestSurtDate() {
        return oldestSurtDate;
    }
}
