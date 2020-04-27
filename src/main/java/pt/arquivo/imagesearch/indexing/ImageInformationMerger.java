package pt.arquivo.imagesearch.indexing;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import pt.arquivo.imagesearch.indexing.data.FullImageMetadata;
import pt.arquivo.imagesearch.indexing.data.ImageData;
import pt.arquivo.imagesearch.indexing.data.PageImageData;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.counters.GenericCounter;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class ImageInformationMerger {

    private FullImageMetadata entry;
    private Reducer.Context context = null;
    private HashMap<Enum<?>, Counter> localCounters;
    private Gson gson;

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

    public void merge(FullImageMetadata fim) {
        if (entry == null)
            entry = fim;
        else
            entry.merge(fim);
    }

    public void reset() {
        entry = null;
    }

    public FullImageMetadata getBestMatch() {
        return entry;
    }
}
