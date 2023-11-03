package pt.arquivo.imagesearch.indexing;

import pt.arquivo.imagesearch.indexing.LocalFullImageIndexer.ReduceDigest;
import pt.arquivo.imagesearch.indexing.data.FullImageMetadata;
import pt.arquivo.imagesearch.indexing.data.ImageData;
import pt.arquivo.imagesearch.indexing.data.MultiPageImageData;
import pt.arquivo.imagesearch.indexing.data.PageImageData;
import pt.arquivo.imagesearch.indexing.data.serializers.ImageDataSerializer;
import pt.arquivo.imagesearch.indexing.data.serializers.MultiPageImageDataSerializer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapreduce.*;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import pt.arquivo.imagesearch.indexing.processors.ImageInformationMerger;
import pt.arquivo.imagesearch.indexing.processors.PageInformationExtractor;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;


/**
 * Runs the full indexing process, similarly to FullImageIndexerJob, but without Hadoop dependencies, running fully locally
 *
 */
public class LocalFullPageIndexer {

    public enum PAGE_INDEXER_COUNTERS {
        application_msword,
        application_pdf,
        application_postscript,
        application_rss_xml,
        application_vnd_ms_excel,
        application_vnd_ms_powerpoint,
        application_vnd_oasis_opendocument_text,
        application_vnd_oasis_opendocument_text_template,
        application_vnd_oasis_opendocument_text_master,
        application_vnd_oasis_opendocument_text_web,
        application_vnd_oasis_opendocument_presentation,
        application_vnd_oasis_opendocument_presentation_template,
        application_vnd_oasis_opendocument_spreadsheet,
        application_vnd_oasis_opendocument_spreadsheet_template,
        application_vnd_sun_xml_calc,
        application_vnd_sun_xml_calc_template,
        application_vnd_sun_xml_impress,
        application_vnd_sun_xml_impress_template,
        application_vnd_sun_xml_writer,
        application_vnd_sun_xml_writer_template,
        application_xhtml_xml,
        application_x_bzip2,
        application_x_gzip,
        application_x_kword,
        application_x_kspread,
        application_x_shockwave_flash,
        application_zip,
        text_html,
        text_plain,
        text_richtext,
        text_rtf,
        text_sgml,
        text_tab_separated_values,
        text_xml
    }


    /**
     * Similar to the Map stage in the ImageIndexerWithDupsJob
     */
    public static class Map {

        private Logger logger = Logger.getLogger(Map.class);
        public String collection;
        public PageInformationExtractor indexer;

        public Map(String collection) {
            this.collection = collection;
            logger.debug(collection + "_Images/img/");
            indexer = new PageInformationExtractor(collection);
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
            String arcName = surl[surl.length - 1];
            String filename = System.currentTimeMillis() + "_" + arcName;
            File dest = new File("/tmp/" + filename);

            try {
                FileUtils.copyURLToFile(url, dest);
                indexer.parseRecord(arcName, dest.getPath());
            } catch (IOException e) {
                e.printStackTrace();
            }
            FileUtils.deleteQuietly(dest);
        }

        
    }

    /**
     * Similar to the Reduce stage in the ImageIndexerWithDupsJob
     */
    public static class Reduce {

        private Logger logger = Logger.getLogger(Reduce.class);
        public ImageInformationMerger merger;

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



    public static void main(String[] args) {
        Logger logger = Logger.getLogger(Map.class);
        logger.setLevel(Level.DEBUG);
        BasicConfigurator.configure();


        assert args.length >= 1 : "Missing file with all arcs path argument";
        String hdfsArcsPath = args[0];

        assert args.length >= 2 : "Missing collection name argument";
        String collection = args[1];

        assert args.length >= 3 : "Missing output file";
        String outputFile = args[2];

        LocalFullPageIndexer.Map map = new Map(collection);

        try (BufferedReader br = new BufferedReader(new FileReader(hdfsArcsPath))) {
            for (String line; (line = br.readLine()) != null; ) {
                if (!line.trim().isEmpty())
                    map.map(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("FullPageIndexer$PAGE_INDEXER_COUNTERS");

        for (PAGE_INDEXER_COUNTERS counter : PAGE_INDEXER_COUNTERS.values()) {
            Counter c = map.indexer.getCounter(counter);
            System.out.println("\t" + c.getName() + ";" + c.getValue());
        }


        System.out.println("FullPageIndexer$MIME_TYPES");


        for (Counter c :  map.indexer.getTmpCounters().values()) {
            System.out.println("\t" + c.getName() + ";" + c.getValue());
        }

    }
}