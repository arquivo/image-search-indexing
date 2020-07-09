package pt.arquivo.imagesearch.indexing;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.archive.io.ArchiveRecord;
import pt.arquivo.imagesearch.indexing.data.FullImageMetadata;
import pt.arquivo.imagesearch.indexing.data.ImageData;
import pt.arquivo.imagesearch.indexing.data.hadoop.WritableArchiveRecord;
import pt.arquivo.imagesearch.indexing.processors.ImageInformationExtractor;
import pt.arquivo.imagesearch.indexing.processors.ImageInformationMerger;
import pt.arquivo.imagesearch.indexing.utils.ImageSearchIndexingUtil;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

public class HDFSImageIndexerWithDupsJob {

    public static class Map extends Mapper<LongWritable, WritableArchiveRecord, Text, Writable> {

        private final Logger logger = Logger.getLogger(Map.class);
        public String collection;
        ImageInformationExtractor indexer;

        @Override
        public void setup(Context context) {
            //logger.setLevel(Level.DEBUG);
            Configuration config = context.getConfiguration();
            collection = config.get("collection");
            logger.debug(collection + "_Images/img/");
            this.collection = config.get("collection");
            indexer = new ImageInformationExtractor(collection, context);
        }

        public void map(LongWritable key, WritableArchiveRecord value, Context context) throws IOException {
            ArchiveRecord rec = value.getRecord();
            indexer.parseRecord(rec);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            logger.info("Cleanup");
            super.cleanup(context);
            for (java.util.Map.Entry<String, FullImageMetadata> entry : indexer.getEntries().entrySet()) {
                String surt = entry.getKey();
                context.write(new Text(surt), entry.getValue());
            }
        }
    }
}