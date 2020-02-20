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
    private int totalMatchingPages;
    private int totalMatchingImages;


    ImageInformationMerger(Reducer.Context context) {
        this.pages = new LinkedList<>();
        this.images = new LinkedList<>();
        this.context = context;
        this.gson = new Gson();

        this.imagesInAllMatchingPages = 0;
        this.totalMatchingPages = 0;
        this.totalMatchingImages = 0;
    }

    ImageInformationMerger() {
        this.pages = new LinkedList<>();
        this.images = new LinkedList<>();
        this.localCounters = new HashMap<>();
        this.gson = new Gson();

        this.imagesInAllMatchingPages = 0;
        this.totalMatchingPages = 0;
        this.totalMatchingImages = 0;
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
        totalMatchingPages += page.getMatchingPages();
        imagesInAllMatchingPages += page.getImagesInAllMatchingPages();
    }

    public void reset() {
        pages = new LinkedList<>();
        images = new LinkedList<>();
        this.imagesInAllMatchingPages = 0;
        this.totalMatchingPages = 0;
        this.totalMatchingImages = 0;
    }

    public List<PageImageData> getPages() {
        return pages;
    }

    public List<ImageData> getImages() {
        return images;
    }

    public FullImageMetadata getBestMatch() {
        ImageData image = images.get(0);

        LocalDateTime timekey = image.getTimestamp();

        PageImageData closestPage = WARCInformationParser.getClosest(pages, timekey);

        return new FullImageMetadata(image, closestPage, totalMatchingImages, totalMatchingPages, imagesInAllMatchingPages);
    }
}
