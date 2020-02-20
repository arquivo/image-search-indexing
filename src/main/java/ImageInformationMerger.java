import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import data.FullImageMetadata;
import data.ImageData;
import data.PageImageData;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import utils.WARCInformationParser;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class ImageInformationMerger {

    private List<PageImageData> pages;
    private List<ImageData> images;
    private Reducer.Context context = null;
    private HashMap<Enum<?>, Counter> localCounters;
    private Gson gson;

    private long imagesInAllMatchingPages;
    private long totalMatchingImgReferences;
    private int totalMatchingPages;
    private int totalMatchingImages;
    private int totalMetadataChanges;


    ImageInformationMerger(Reducer.Context context) {
        this.context = context;
        this.gson = new Gson();
        reset();
    }

    ImageInformationMerger() {
        this.localCounters = new HashMap<>();
        this.gson = new Gson();
        reset();
    }

    public Counter getCounter(Enum<?> counterName) {
        if (context != null) {
            return context.getCounter(counterName);
        } else {
            if (localCounters.get(counterName) == null)
                localCounters.put(counterName, new GenericCounter(counterName.name(), counterName.name()));
            return localCounters.get(counterName);
        }
    }

    public void add(Text val) {
        try {
            PageImageData page = gson.fromJson(val.toString(), PageImageData.class);
            if (page.getType() == null || !page.getType().equals("page"))
                throw new JsonSyntaxException("");
            addPage(page);

        } catch (JsonSyntaxException e) {
            ImageData image = gson.fromJson(val.toString(), ImageData.class);
            addImage(image);
        }
    }

    public void add(Object val) {
        if (val.getClass() == PageImageData.class)
            addPage((PageImageData) val);
        if (val.getClass() == ImageData.class)
            addImage((ImageData) val);
    }

    public void addImage(ImageData image) {
        images.add(image);
        totalMatchingImages += image.getMatchingImages();
    }

    public void addPage(PageImageData page) {
        pages.add(page);
        totalMatchingImgReferences += page.getTotalMatchingImgReferences();
        imagesInAllMatchingPages += page.getImagesInAllMatchingPages();
        totalMatchingPages += page.getMatchingPages();
        totalMetadataChanges += page.getMetadataChanges();
    }

    public void reset() {
        pages = new LinkedList<>();
        images = new LinkedList<>();
        this.imagesInAllMatchingPages = 0;
        this.totalMatchingImgReferences = 0;
        this.totalMatchingImages = 0;
        this.totalMatchingPages = 0;
        this.totalMetadataChanges = 0;
    }

    public List<PageImageData> getPages() {
        return pages;
    }

    public List<ImageData> getImages() {
        return images;
    }

    public FullImageMetadata getBestMatch() {
        ImageData imageData = images.get(0);

        for (ImageData image : images.subList(1, images.size())) {
            imageData.addTimestamps(image.getTimestamp());
            imageData.addContentHashes(image.getContentHash());
        }

        PageImageData pageData = pages.get(0);

        for (PageImageData page : pages.subList(1, pages.size())) {
            pageData.addPageImageData(page);
        }


        return new FullImageMetadata(imageData, pageData, totalMatchingImages, totalMatchingPages, totalMatchingImgReferences, imagesInAllMatchingPages, totalMetadataChanges);
    }
}
