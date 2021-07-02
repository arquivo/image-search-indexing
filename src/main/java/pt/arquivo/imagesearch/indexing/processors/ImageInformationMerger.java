package pt.arquivo.imagesearch.indexing.processors;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.apache.log4j.Logger;
import pt.arquivo.imagesearch.indexing.DupDigestMergerJob;
import pt.arquivo.imagesearch.indexing.data.FullImageMetadata;

import java.util.HashMap;
import java.util.List;

/**
 * Merges FullImageMetadata records for deduplication
 */
public class ImageInformationMerger {

    private final org.apache.log4j.Logger logger = Logger.getLogger(ImageInformationMerger.class);

    /**
     * Maximum supported number of objects to merge
     */
    public static final int MAX_OBJECTS_TO_MERGE = 10000;


    /**
     * Metadata entry that will contain the merged record
     */
    private FullImageMetadata entry;

    /**
     * Hadoop context
     */
    private Reducer.Context context = null;

    /**
     * Local counter cache
     */
    private HashMap<Enum<?>, Counter> localCounters;

    /**
     * Constructor for Hadoop
     *
     * @param context Hadoop context
     */
    public ImageInformationMerger(Reducer.Context context) {
        this.context = context;
        reset();
    }

    /**
     * Non-Hadoop constructor
     */
    public ImageInformationMerger() {
        this.localCounters = new HashMap<>();
        reset();
    }

    /**
     * Get counters from Hadoop or from local cache
     *
     * @param counterName name of counter
     * @return counter object
     */
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

    /**
     * Reset the Merger to work with a new FullImageMetadata object
     */
    public void reset() {
        entry = null;
    }

    /**
     * Create best object from the candidate objects
     *
     * @return the best matched object
     */
    public FullImageMetadata getBestMatch() {
        entry.assignImagesToPages();
        return entry;
    }

    /**
     * Merge all objects into the current FullImageMetadata object
     *
     * @param values FullImageMetadata values to merge
     * @return merged object
     */
    public int mergeAll(Iterable<FullImageMetadata> values) {
        int counter = 0;
        for (FullImageMetadata metadata : values) {
            merge(metadata);
            counter++;
        }
        return counter;
    }

    /**
     * Merge all objects into the current FullImageMetadata object
     * (to be used in Haddop)
     *
     * @param values FullImageMetadata values to merge
     * @return merged object
     */
    public int mergeAllHadoop(Iterable<Writable> values) {
        int counter = 0;
        for (Writable value : values) {
            FullImageMetadata metadata = (FullImageMetadata) value;
            merge(metadata);
            counter++;
            if (counter >= MAX_OBJECTS_TO_MERGE)
                break;
        }
        return counter;
    }

    /**
     * Merge all objects into the current FullImageMetadata object
     *
     * @param values FullImageMetadata values to merge
     * @return merged object
     */
    public int mergeAll(List<Object> values) {
        int counter = 0;
        for (Object value : values) {
            FullImageMetadata metadata = (FullImageMetadata) value;
            merge(metadata);
            counter++;
            if (counter >= MAX_OBJECTS_TO_MERGE)
                break;
        }
        return counter;
    }
}
