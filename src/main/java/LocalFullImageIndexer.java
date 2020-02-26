import com.google.gson.Gson;
import data.ImageData;
import data.PageImageData;
import org.apache.hadoop.mapreduce.*;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class LocalFullImageIndexer {

    public static class Map {

        private Logger logger = Logger.getLogger(Map.class);
        public String collection;
        ImageInformationExtractor indexer;

        public Map(String collection) {
            //logger.setLevel(Level.DEBUG);
            this.collection = collection;
            logger.debug(collection + "_Images/img/");
            indexer = new ImageInformationExtractor(collection);
        }

        public void map(String arcURL) {
            logger.info("(W)ARCNAME: " + arcURL);
            indexer.getCounter(FullImageIndexer.IMAGE_COUNTERS.WARCS).increment(1);
            indexer.parseRecord(arcURL);
        }

        public HashMap<String, List<Object>> cleanup() {
            HashMap<String, List<Object>> results = new HashMap<>();
            for (java.util.Map.Entry<String, PageImageData> entry : indexer.getImgSrcEntries().entrySet()) {
                String surt = entry.getKey();
                results.computeIfAbsent(surt, k -> new LinkedList<>());
                results.get(surt).add(entry.getValue());
                //context.write(new Text(surt), new Text(gson.toJson(entry.getValue())));
            }

            for (java.util.Map.Entry<String, ImageData> entry : indexer.getImgFileEntries().entrySet()) {
                String surt = entry.getKey();
                results.computeIfAbsent(surt, k -> new LinkedList<>());
                results.get(surt).add(entry.getValue());
                //context.write(new Text(surt), new Text(gson.toJson(entry.getValue())));
            }
            return results;
        }
    }

    public static class Reduce {

        private Logger logger = Logger.getLogger(Reduce.class);
        private ImageInformationMerger merger;
        private Gson gson;

        public Reduce() {
            merger = new ImageInformationMerger();
            gson = new Gson();
        }


        public String reduce(String key, List<Object> values) {

            int counter = 0;

            merger.reset();
            //TODO: check http://codingjunkie.net/secondary-sort  to see if it helps not having to iterate all records
            logger.debug("Reducing: " + key);
            for (Object val : values) {
                merger.add(val);
                counter++;
                if (counter >= 1000) {
                    logger.info(String.format("Broke iterating: %d pages and %d images", merger.getPages().size(), merger.getImages().size()));
                    merger.getCounter(FullImageIndexer.REDUCE_COUNTERS.IMAGES_PAGES_EXCEEDED).increment(1);
                    break;
                }
            }
            logger.debug(String.format("Found %d pages and %d images", merger.getPages().size(), merger.getImages().size()));
            if (merger.getImages().size() != 0 && merger.getPages().size() != 0) {
                for (PageImageData page : merger.getPages())
                    merger.getCounter(FullImageIndexer.REDUCE_COUNTERS.URL_IMAGES_PAGESALL).increment(page.getMatchingPages());

                for (ImageData image : merger.getImages())
                    merger.getCounter(FullImageIndexer.REDUCE_COUNTERS.URL_IMAGESALL_PAGES).increment(image.getMatchingImages());

                merger.getCounter(FullImageIndexer.REDUCE_COUNTERS.URL_IMAGES_PAGES).increment(1);

                //logger.debug(String.format("%s: Found %d images and %d pages; image TS: \"%s\" page TS: \"%s\"", key, images.size(), pages.size(), images.get(0) == null ? "none" : images.get(0).getTimestamp().toString(), pages.get(0) == null ? "none" : pages.get(0).getTimestamp().toString()));

                return gson.toJson(merger.getBestMatch());
            } else if (merger.getImages().size() != 0) {
                merger.getCounter(FullImageIndexer.REDUCE_COUNTERS.URL_IMAGES_NPAGES).increment(1);
                for (ImageData image : merger.getImages())
                    merger.getCounter(FullImageIndexer.REDUCE_COUNTERS.URL_IMAGESALL_NPAGES).increment(image.getMatchingImages());

            } else if (merger.getPages().size() != 0) {

                merger.getCounter(FullImageIndexer.REDUCE_COUNTERS.URL_NIMAGES_PAGES).increment(1);

                for (PageImageData page : merger.getPages())
                    merger.getCounter(FullImageIndexer.REDUCE_COUNTERS.URL_NIMAGES_PAGESALL).increment(page.getMatchingPages());
            }
            return null;
        }


    }

    public static void main(String[] args) {
        assert args.length >= 1 : "Missing hdfs file with all arcs path argument";
        String hdfsArcsPath = args[0];

        assert args.length >= 2 : "Missing collection name argument";
        String collection = args[1];

        assert args.length >= 3 : "Missing output file";
        String outputFile = args[2];

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

        LocalFullImageIndexer.Reduce reduce = new Reduce();

        try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(outputFile)))) {

            for (java.util.Map.Entry<String, List<Object>> entry : mapResults.entrySet()) {
                String result = reduce.reduce(entry.getKey(), entry.getValue());
                if (result != null)
                    out.println(result);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("FullImageIndexer$IMAGE_COUNTERS");

        for (FullImageIndexer.IMAGE_COUNTERS counter : FullImageIndexer.IMAGE_COUNTERS.values()) {
            Counter c = map.indexer.getCounter(counter);
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        System.out.println("FullImageIndexer$PAGE_COUNTERS");
        for (FullImageIndexer.PAGE_COUNTERS counter : FullImageIndexer.PAGE_COUNTERS.values()) {
            Counter c = map.indexer.getCounter(counter);
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        System.out.println("FullImageIndexer$REDUCE_COUNTERS");
        for (FullImageIndexer.REDUCE_COUNTERS counter : FullImageIndexer.REDUCE_COUNTERS.values()) {
            Counter c = reduce.merger.getCounter(counter);
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

    }
}