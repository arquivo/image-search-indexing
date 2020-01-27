import java.io.IOException;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.Logger;
import org.archive.io.arc.ARCRecord;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.DuplicateKeyException;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;

class ImageMap extends Mapper<LongWritable, Text, LongWritable, NullWritable> {
    private Logger logger = Logger.getLogger(ImageMap.class);
    public String collection;

    private MongoClient mongoClient;
    private DBCollection imagesMongoDB;

    @Override
    public void setup(Context context) {
        Configuration config = context.getConfiguration();
        this.collection = config.get("collection");
        System.out.println("collection: " + this.collection);

        String mongodbServers = config.get("mondodb.servers");
        List<ServerAddress> mongodbServerSeeds = ImageSearchIndexingUtil.getMongoDBServerAddresses(mongodbServers);

        MongoClientOptions.Builder options = MongoClientOptions.builder();
        options.socketKeepAlive(true);
        mongoClient = new MongoClient(mongodbServerSeeds, options.build());
        DB database = mongoClient.getDB("hadoop_images");
        imagesMongoDB = database.getCollection("images");
    }

    @Override
    protected void cleanup(Mapper<LongWritable, Text, LongWritable, NullWritable>.Context context)
            throws IOException, InterruptedException {

        mongoClient.close();
        super.cleanup(context);
    }

    public void createImageDB(String arcURL, WARCRecordResponseEncapsulated record, Context context) {
        String url = record.getWARCRecord().getHeader().getUrl();
        String tstamp = record.getTs();
        String mime = record.getContentMimetype();

        String image_hash_key = ImageSearchIndexingUtil.md5ofString(url);
        String content_hash = ImageSearchIndexingUtil.md5ofString(tstamp + "/" + url);
        byte[] contentBytes = null;
        contentBytes = record.getContentBytes();

        DBObject img = new BasicDBObject("_id", new BasicDBObject("image_hash_key", image_hash_key).append("tstamp", tstamp))
                .append("url", url)
                .append("tstamp", tstamp)
                .append("mime", mime)
                .append("collection", this.collection)
                .append("safe", -1)
                .append("content_hash", content_hash)
                .append("bytes64string", Base64.encodeBase64String(contentBytes));
        imagesMongoDB.insert(img);
    }

    public void createImageDB(String arcURL, ARCRecord record, Context context) {
        String url = record.getHeader().getUrl();
        String tstamp = record.getMetaData().getDate();
        String mime = record.getMetaData().getMimetype();
        String image_hash_key = ImageSearchIndexingUtil.md5ofString(url);
        String content_hash = ImageSearchIndexingUtil.md5ofString(tstamp + "/" + url);
        byte[] contentBytes;
        try {
            contentBytes = ImageSearchIndexingUtil.getRecordContentBytes(record);
        } catch (IOException e) {
            logger.error(String.format("Error getting record content bytes for image url: %s on offset %d with error message %s", url, record.getBodyOffset(), e.getMessage()));
            return;
        }
        DBObject img = new BasicDBObject("_id", new BasicDBObject("image_hash_key", image_hash_key).append("tstamp", tstamp))
                .append("url", url)
                .append("tstamp", tstamp)
                .append("mime", mime)
                .append("collection", this.collection)
                .append("safe", -1)
                .append("content_hash", content_hash)
                .append("bytes64string", Base64.encodeBase64String(contentBytes));
        try {
            imagesMongoDB.insert(img);
        } catch (DuplicateKeyException e) {
            logger.debug("Image already exists " + e.getMessage());
        }
    }

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String arcURL = value.toString();

        System.out.println("(W)ARCNAME: " + arcURL);
        logger.info("(W)ARCNAME: " + arcURL);

        if (arcURL.endsWith("warc.gz") || arcURL.endsWith("warc")) {
            ImageSearchIndexingUtil.readWarcRecords(arcURL, (record) -> {
                boolean isImage = record.getContentMimetype().contains("image");
                if (isImage) {
                    createImageDB(arcURL, record, context);
                }
            });
        } else {
            ImageSearchIndexingUtil.readArcRecords(arcURL, record -> {
                boolean isImage = record.getMetaData().getMimetype().contains("image");
                if (isImage) {
                    createImageDB(arcURL, record, context);
                }
            });
        }

    }

}

// Empty reducer
// Improve by telling hadoop to run a job without a reducer
class ImageMapReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
}

public class CreateImageDB {
    public static void main(String[] args) throws Exception {
        assert args.length >= 1 : "Missing hdfs file with all arcs path argument";
        String hdfsArcsPath = args[0];

        assert args.length >= 2 : "Missing collection name argument";
        String collection = args[1];
        String jobName = collection + "_CreateImageDB_1";

        assert args.length >= 3 : "Missing mondo DB servers connection string argument";
        String mongodbServers = args[2];

        assert args.length >= 4 : "Missing argument max running map in parallel";
        int maxMaps = Integer.parseInt(args[3]);

        assert args.length >= 5 : "Missing argument max arcs per map";
        int linespermap = Integer.parseInt(args[4]);

        Configuration conf = new Configuration();
        conf.set("collection", collection);
        conf.set("mapred.job.priority", JobPriority.VERY_HIGH.toString());
        conf.set("mondodb.servers", mongodbServers);

        Job job = new Job(conf, "Mapper_Only_Job");
        job.setJarByClass(CreateImageDB.class);
        job.setMapperClass(ImageMap.class);
        job.setJobName(jobName);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setReducerClass(ImageMapReducer.class);
        job.setInputFormatClass(NLineInputFormat.class);

        NLineInputFormat.addInputPath(job, new Path(hdfsArcsPath));

        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", linespermap);
        job.getConfiguration().setInt("mapreduce.job.running.map.limit", maxMaps); /*Maximum of 500 simultaneous maps accessing preprod for now*/

        // Sets reducer tasks to 1
        job.setNumReduceTasks(1);

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
