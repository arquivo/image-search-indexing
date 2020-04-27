package pt.arquivo.imagesearch.indexing.data;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.time.LocalDateTime;
import java.util.*;

public class FullImageMetadata {

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
        for (ImageData data : result.getImageDatas())
            addImageData(data);

        for (PageImageData data : result.getPageImageDatas())
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

    public Collection<ImageData> getImageDatas() {
        return imageDatas.values();
    }

    public Collection<PageImageData> getPageImageDatas() {
        return pageImageDatas.values();
    }

    public boolean hasImageMetadata() {
        for (PageImageData pid : pageImageDatas.values())
            if (!pid.getImageMetadata().isEmpty() || !pid.getImageMetadata().isEmpty())
                return true;
        return false;
    }

    public void assignImagesToPages() {
        TreeMap<LocalDateTime, ImageData> map = new TreeMap<>();
        for (ImageData data : this.getImageDatas())
            for (LocalDateTime timestamp : data.getTimestamp())
                map.put(timestamp, data);

        for (PageImageData data : this.getPageImageDatas()) {
            LocalDateTime timestamp = data.getPageTimestamp();

            LocalDateTime before = map.floorKey(timestamp);
            LocalDateTime after = map.ceilingKey(timestamp);
            LocalDateTime correct = null;
            if (before == null) correct = after;
            else if (after == null) correct = before;
            else if ((after.compareTo(timestamp)) < (timestamp.compareTo(before))) timestamp = after;
            else timestamp = before;

            data.setImgTimestamp(timestamp);

        }
    }
}

