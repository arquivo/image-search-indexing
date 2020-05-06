package pt.arquivo.imagesearch.indexing.processors;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import pt.arquivo.imagesearch.indexing.DupDigestMergerJob;
import pt.arquivo.imagesearch.indexing.data.FullImageMetadata;
import pt.arquivo.imagesearch.indexing.data.ImageData;
import pt.arquivo.imagesearch.indexing.data.PageImageData;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.counters.GenericCounter;

import java.awt.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class ImageInformationMerger {

    private final org.apache.log4j.Logger logger = Logger.getLogger(ImageInformationMerger.class);

    private FullImageMetadata entry;
    private Reducer.Context context = null;
    private HashMap<Enum<?>, Counter> localCounters;

    public ImageInformationMerger(Reducer.Context context) {
        this.context = context;
        reset();
    }

    public ImageInformationMerger() {
        this.localCounters = new HashMap<>();
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

    public void merge(FullImageMetadata metadata) {
        getCounter(DupDigestMergerJob.COUNTERS.RECORDS_IN).increment(1);

        if (entry == null) {
            entry = new FullImageMetadata(metadata);
        } else {
            entry.merge(metadata);
        }

        getCounter(DupDigestMergerJob.COUNTERS.URL_IMAGES_PAGESALL).increment(metadata.getPageImageDatasValues().size());
        getCounter(DupDigestMergerJob.COUNTERS.URL_IMAGESALL_PAGES).increment(metadata.getImageDatasValues().size());
        getCounter(DupDigestMergerJob.COUNTERS.URL_IMAGES_PAGES).increment(1);
    }

    public void reset() {
        entry = null;
    }

    public FullImageMetadata getBestMatch() {
        entry.assignImagesToPages();
        return entry;
    }

    public int mergeAll(Iterable<FullImageMetadata> values) {
        int counter = 0;
        for (FullImageMetadata metadata : values) {
            merge(metadata);
            counter++;
        }
        return counter;
    }

    public int mergeAllHadoop(Iterable<Writable> values) {
        int counter = 0;
        for (Writable value : values) {
            FullImageMetadata metadata = (FullImageMetadata) value;
            merge(metadata);
            counter++;
            if (counter >= 10000)
                break;
        }
        return counter;
    }

    public int mergeAll(List<Object> values) {
        int counter = 0;
        for (Object value : values) {
            FullImageMetadata metadata = (FullImageMetadata) value;
            merge(metadata);
            counter++;
            if (counter >= 10000)
                break;
        }
        return counter;
    }
}
