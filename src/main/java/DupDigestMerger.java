import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.sun.jersey.core.util.Base64;
import data.FullImageMetadata;
import data.ImageData;
import data.PageImageData;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.apache.log4j.Logger;
import org.archive.io.arc.ARCRecord;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import utils.WARCInformationParser;
import utils.WARCRecordResponseEncapsulated;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class DupDigestMerger {

    private Logger logger = Logger.getLogger(DupDigestMerger.class);

    private String collection;
    private Mapper<LongWritable, Text, Text, Text>.Context context;
    private HashMap<Enum<?>, Counter> localCounters;
    private Gson gson;

    public DupDigestMerger() {
        this.collection = collection;
        this.localCounters = new HashMap<>();
        this.gson = new Gson();
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

    public FullImageMetadata parseRecord(Text recordJson) {
        FullImageMetadata page = null;
        try {
            page = gson.fromJson(recordJson.toString(), FullImageMetadata.class);
        } catch (JsonSyntaxException e) {

        }
        return page;
    }


}
