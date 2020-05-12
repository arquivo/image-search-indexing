package pt.arquivo.imagesearch.indexing;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.log4j.Level;
import pt.arquivo.imagesearch.indexing.data.FullImageMetadata;
import pt.arquivo.imagesearch.indexing.data.ImageData;
import pt.arquivo.imagesearch.indexing.data.PageImageData;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.log4j.Logger;
import pt.arquivo.imagesearch.indexing.data.serializers.ImageDataSerializer;
import pt.arquivo.imagesearch.indexing.data.serializers.PageImageDataSerializer;
import pt.arquivo.imagesearch.indexing.processors.ImageInformationExtractor;
import pt.arquivo.imagesearch.indexing.processors.ImageInformationMerger;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

public class LocalFullImageIndexer {

    public static class Map {

        private Logger logger = Logger.getLogger(Map.class);
        public String collection;
        ImageInformationExtractor indexer;

        public Map(String collection) {
            this.collection = collection;
            logger.debug(collection + "_Images/img/");
            indexer = new ImageInformationExtractor(collection);
        }

        public void map(String arcURL) {
            logger.info("(W)ARCNAME: " + arcURL);
            indexer.getCounter(ImageIndexerWithDupsJob.IMAGE_COUNTERS.WARCS).increment(1);

            URL url = null;
            try {
                url = new URL(arcURL);
            } catch (MalformedURLException ignored) {

            }

            String[] surl = url.getPath().split("/");
            String filename = System.currentTimeMillis() + "_" + surl[surl.length - 1];
            File dest = new File("/tmp/" + filename);

            try {
                FileUtils.copyURLToFile(url, dest);
                indexer.parseRecord(dest.getPath());
            } catch (IOException e) {
                e.printStackTrace();
            }
            FileUtils.deleteQuietly(dest);
        }

        public HashMap<String, List<Object>> cleanup() {
            HashMap<String, List<Object>> results = new HashMap<>();
            for (java.util.Map.Entry<String, FullImageMetadata> entry : indexer.getEntries().entrySet()) {
                String surt = entry.getKey();
                results.computeIfAbsent(surt, k -> new LinkedList<>());
                results.get(surt).add(entry.getValue());
            }
            return results;
        }
    }

    public static class Reduce {

        private Logger logger = Logger.getLogger(Reduce.class);
        private ImageInformationMerger merger;

        public Reduce() {
            merger = new ImageInformationMerger();
        }

        public Counter getCounter(Enum<?> counterName) {
            return merger.getCounter(counterName);
        }


        public FullImageMetadata reduce(String key, List<Object> values) {

            merger.reset();
            int counter = merger.mergeAll(values);
            FullImageMetadata result = merger.getBestMatch();
            logger.debug(String.format("Found %d pages and %d images", result.getPageImageDatasValues().size(), result.getImageDatasValues().size()));

            if (result.getImageDatasValues().size() != 0 && result.getPageImageDatasValues().size() != 0) {
                merger.getCounter(ImageIndexerWithDupsJob.REDUCE_COUNTERS.URL_IMAGES_PAGESALL).increment(result.getPageImageDatasValues().size());
                merger.getCounter(ImageIndexerWithDupsJob.REDUCE_COUNTERS.URL_IMAGESALL_PAGES).increment(result.getImageDatasValues().size());
                merger.getCounter(ImageIndexerWithDupsJob.REDUCE_COUNTERS.URL_IMAGES_PAGES).increment(1);
                //logger.debug(String.format("%s: Found %d images and %d pages; image TS: \"%s\" page TS: \"%s\"", key, images.size(), pages.size(), images.get(0) == null ? "none" : images.get(0).getTimestamp().toString(), pages.get(0) == null ? "none" : pages.get(0).getTimestamp().toString()));
                return result;
            } else if (result.getImageDatasValues().size() != 0) {
                merger.getCounter(ImageIndexerWithDupsJob.REDUCE_COUNTERS.URL_IMAGES_NPAGES).increment(1);
                merger.getCounter(ImageIndexerWithDupsJob.REDUCE_COUNTERS.URL_IMAGESALL_NPAGES).increment(result.getImageDatasValues().size());
            } else if (result.getPageImageDatasValues().size() != 0) {
                merger.getCounter(ImageIndexerWithDupsJob.REDUCE_COUNTERS.URL_NIMAGES_PAGES).increment(1);
                merger.getCounter(ImageIndexerWithDupsJob.REDUCE_COUNTERS.URL_NIMAGES_PAGESALL).increment(result.getPageImageDatasValues().size());
            }
            return null;
        }


    }

    public static class ReduceDigest {

        private final Logger logger = Logger.getLogger(ReduceDigest.class);
        public String collection;
        private ImageInformationMerger merger;

        public ReduceDigest() {
            merger = new ImageInformationMerger();
        }


        public FullImageMetadata reduce(Text key, Iterable<FullImageMetadata> values) {
            logger.debug("Reducing: " + key);

            merger.reset();
            int counter = merger.mergeAll(values);
            FullImageMetadata result = merger.getBestMatch();

            merger.getCounter(DupDigestMergerJob.COUNTERS.RECORDS_OUT).increment(1);
            merger.getCounter(DupDigestMergerJob.REDUCE_COUNTERS.URL_IMAGES_PAGESALL).increment(result.getPageImageDatasValues().size());
            merger.getCounter(DupDigestMergerJob.REDUCE_COUNTERS.URL_IMAGESALL_PAGES).increment(result.getImageDatasValues().size());
            merger.getCounter(DupDigestMergerJob.REDUCE_COUNTERS.URL_IMAGES_PAGES).increment(1);

            if (result.hasImageMetadata())
                merger.getCounter(DupDigestMergerJob.COUNTERS.RECORDS_WITH_METADATA).increment(1);
            else
                merger.getCounter(DupDigestMergerJob.COUNTERS.RECORDS_WITHOUT_METADATA).increment(1);

            return result;

        }
    }

    public static void main(String[] args) {
        Logger logger = Logger.getLogger(Map.class);
        logger.setLevel(Level.INFO);

        assert args.length >= 1 : "Missing hdfs file with all arcs path argument";
        String hdfsArcsPath = args[0];

        assert args.length >= 2 : "Missing collection name argument";
        String collection = args[1];

        assert args.length >= 3 : "Missing output file";
        String outputFile = args[2];

        Gson gson = new GsonBuilder()
                .registerTypeAdapter(PageImageData.class, new PageImageDataSerializer())
                .registerTypeAdapter(ImageData.class, new ImageDataSerializer())
                .create();

        LocalFullImageIndexer.Map map = new Map(collection);

        try (BufferedReader br = new BufferedReader(new FileReader(hdfsArcsPath))) {
            for (String line; (line = br.readLine()) != null; ) {
                if (!line.trim().isEmpty())
                    map.map(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        HashMap<String, List<Object>> mapResults = map.cleanup();
        HashMap<String, List<FullImageMetadata>> reduceResults = new HashMap<>();

        LocalFullImageIndexer.Reduce reduce = new Reduce();
        LocalFullImageIndexer.ReduceDigest reduceDigest = new ReduceDigest();

        for (java.util.Map.Entry<String, List<Object>> entry : mapResults.entrySet()) {
            FullImageMetadata result = reduce.reduce(entry.getKey(), entry.getValue());
            if (result != null) {
                Set<String> digests = new HashSet<>();
                for (ImageData imageData : result.getImageDatasValues()) {
                    String digest = imageData.getContentHash();
                    if (!digests.contains(imageData.getContentHash())) {
                        reduce.getCounter(DupDigestMergerJob.COUNTERS.RECORDS_MAP_IN).increment(1);
                        FullImageMetadata resultDigest = new FullImageMetadata(result, imageData);
                        reduceResults.putIfAbsent(digest, new LinkedList<>());
                        reduceResults.get(digest).add(resultDigest);
                        digests.add(digest);
                    }
                }
            }
        }



        try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(outputFile)))) {
            for (java.util.Map.Entry<String, List<FullImageMetadata>> entry : reduceResults.entrySet()) {

                FullImageMetadata result = reduceDigest.reduce(new Text(entry.getKey()), entry.getValue());
                if (result != null) {
                    for (ImageData data : result.getImageDatasValues())
                        out.println(gson.toJson(data));
                    for (PageImageData data : result.getPageImageDatasValues())
                        out.println(gson.toJson(data));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


        System.out.println("FullImageIndexer$IMAGE_COUNTERS");

        for (ImageIndexerWithDupsJob.IMAGE_COUNTERS counter : ImageIndexerWithDupsJob.IMAGE_COUNTERS.values()) {
            Counter c = map.indexer.getCounter(counter);
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        System.out.println("FullImageIndexer$PAGE_COUNTERS");
        for (ImageIndexerWithDupsJob.PAGE_COUNTERS counter : ImageIndexerWithDupsJob.PAGE_COUNTERS.values()) {
            Counter c = map.indexer.getCounter(counter);
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        System.out.println("FullImageIndexer$REDUCE_COUNTERS");
        for (ImageIndexerWithDupsJob.REDUCE_COUNTERS counter : ImageIndexerWithDupsJob.REDUCE_COUNTERS.values()) {
            Counter c = reduce.merger.getCounter(counter);
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        System.out.println("DupDigestMergerJob$COUNTERS");
        for (DupDigestMergerJob.COUNTERS counter : DupDigestMergerJob.COUNTERS.values()) {
            Counter c = reduceDigest.merger.getCounter(counter);
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        System.out.println("DupDigestMergerJob$REDUCE_COUNTERS");
        for (DupDigestMergerJob.REDUCE_COUNTERS counter : DupDigestMergerJob.REDUCE_COUNTERS.values()) {
            Counter c = reduceDigest.merger.getCounter(counter);
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

    }
}
