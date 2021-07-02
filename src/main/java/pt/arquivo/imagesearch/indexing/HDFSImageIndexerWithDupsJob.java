package pt.arquivo.imagesearch.indexing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.archive.io.ArchiveRecord;
import pt.arquivo.imagesearch.indexing.data.FullImageMetadata;
import pt.arquivo.imagesearch.indexing.data.hadoop.WritableArchiveRecord;
import pt.arquivo.imagesearch.indexing.processors.ImageInformationExtractor;

import java.io.IOException;


/**
 * First stage Hadoop process to similar to the ImageIndexerWithDupsJob, but used for when (W)ARC files are in HDFS
 * In my experiments, using (W)ARCs already in HDFS was not much faster than using (W)ARCs in the document server
 */
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


        /**
         * First stage hadoop processing
         *
         * Processes it into all required metadata.
         * Entries are written into their corresponding SURT entries
         *
         * @param key Hadoop key (not required at this stage)
         * @param value (W)ARC record ready for parsing
         * @param context Hadoop context
         * @throws IOException unrecoverable errors processing (W)ARCs are thrown so that Hadoop retries it
         */
        public void map(LongWritable key, WritableArchiveRecord value, Context context) throws IOException {
            ArchiveRecord rec = value.getRecord();
            indexer.parseRecord(rec);
        }

        /**
         * So, results are only written at Hadoop cleanup stage (after all maps are finished) so that fewer duplicates are sent to the next stage
         *
         * @param context Hadoop context which will contain all the processed metadata
         * @throws IOException error writing to Hadoop context
         * @throws InterruptedException error calling parent super.cleanup
         */
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