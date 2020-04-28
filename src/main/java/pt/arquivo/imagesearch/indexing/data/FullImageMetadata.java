package pt.arquivo.imagesearch.indexing.data;

import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import pt.arquivo.imagesearch.indexing.ImageInformationExtractor;

import java.io.*;
import java.time.LocalDateTime;
import java.util.*;

public class FullImageMetadata implements Writable, Serializable {

    private transient Logger logger = Logger.getLogger(FullImageMetadata.class);

    public static final int MAXIMUM_META = 50;

    private HashMap<ImageData, ImageData> imageDatas;
    private HashMap<PageImageData, PageImageData> pageImageDatas;

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
        this.imageDatas = new HashMap<>();
        this.pageImageDatas = new HashMap<>();
    }


    public void merge(FullImageMetadata result) {
        for (ImageData data : result.getImageDatasValues())
            addImageData(data);

        for (PageImageData data : result.getPageImageDatasValues())
            addPageImageData(data);
    }

    public boolean addImageData(ImageData imageData) {
        this.matchingImages++;

        if (imageDatas.get(imageData) != null) {
            imageDatas.get(imageData).addTimestamp(imageData);
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

    public HashMap<ImageData, ImageData> getImageDatas() {
        return imageDatas;
    }

    public Collection<PageImageData> getPageImageDatasValues() {
        return pageImageDatas.values();
    }

    public HashMap<PageImageData, PageImageData> getPageImageDatas() {
        return pageImageDatas;
    }

    public boolean hasImageMetadata() {
        for (PageImageData pid : pageImageDatas.values())
            if (!pid.getImageMetadata().isEmpty() || !pid.getImageMetadata().isEmpty())
                return true;
        return false;
    }

    public void assignImagesToPages() {
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
            this.imageMetadataChanges = other.getPageMetadataChanges();
            this.pageMetadataChanges = other.getPageMetadataChanges();

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
}

