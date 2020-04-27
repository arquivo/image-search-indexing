package pt.arquivo.imagesearch.indexing;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import pt.arquivo.imagesearch.indexing.data.FullImageMetadata;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.apache.log4j.Logger;

import java.util.HashMap;

public class DupDigestMerger {

    private Logger logger = Logger.getLogger(DupDigestMerger.class);

    private Mapper<LongWritable, Text, Text, Text>.Context context;
    private HashMap<Enum<?>, Counter> localCounters;

    public DupDigestMerger() {
        this.localCounters = new HashMap<>();

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

    public static FullImageMetadata parseRecord(Text recordJson) {
        FullImageMetadata page = null;
        try {
            page = new Gson().fromJson(recordJson.toString(), FullImageMetadata.class);
        } catch (JsonSyntaxException ignored) {
            ignored.printStackTrace();

        }
        return page;
    }


}
