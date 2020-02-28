import com.google.gson.Gson;
import data.FullImageMetadata;
import data.ImageData;
import data.PageImageData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

public class DupDigestMergerJob {

    public enum COUNTERS {
        RECORDS_EXCEEDED,
        RECORDS_IN,
        RECORDS_OUT
    }

    public static class Map extends Mapper<Text, Text, Text, Text> {

        private final Logger logger = Logger.getLogger(Map.class);

        public void map(Text key, Text value, Context context) {
            try {
                context.write(key, value);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, NullWritable, Text> {

        private final Logger logger = Logger.getLogger(Reduce.class);
        public String collection;
        private DupDigestMerger merger;

        @Override
        public void setup(Reducer.Context context) {
            merger = new DupDigestMerger();
        }


        public void reduce(Text key, Iterable<Text> values, Context context) {
            int counter = 0;
            Gson gson = new Gson();
            FullImageMetadata result = null;

            for (Text val : values) {
                context.getCounter(COUNTERS.RECORDS_IN).increment(1);
                FullImageMetadata metadata = merger.parseRecord(val);
                if (result == null) {
                    context.getCounter(COUNTERS.RECORDS_OUT).increment(1);
                    result = metadata;
                } else {
                    result.merge(metadata);
                }
                if (counter >= 1000) {
                    logger.info(String.format("Broke iterating: %d records", counter));
                    context.getCounter(COUNTERS.RECORDS_EXCEEDED).increment(1);
                    break;
                }
                counter++;


            }
            logger.info(String.format("Found %d records", counter));

            try {
                context.write(NullWritable.get(), new Text(gson.toJson(result)));
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }

        }


    }

    public static void main(String[] args) throws Exception {
        assert args.length >= 1 : "Missing hdfs file with all arcs path argument";
        String hdfsArcsPath = args[0];

        assert args.length >= 2 : "Missing collection name argument";
        String collection = args[1];
        String jobName = collection + "_FullIndexer";

        assert args.length >= 3 : "Missing number of warcs per map";
        int linesPerMap = Integer.parseInt(args[2]);

        assert args.length >= 4 : "Missing number of reduces";
        int reducesCount = Integer.parseInt(args[3]);


        Configuration conf = new Configuration();
        conf.set("collection", collection);

        Job job = Job.getInstance(conf);
        job.setJarByClass(DupDigestMergerJob.class);
        job.setInputFormatClass(NLineInputFormat.class);

        job.setMapperClass(DupDigestMergerJob.Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(DupDigestMergerJob.Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setJobName(jobName);

        NLineInputFormat.addInputPath(job, new Path(hdfsArcsPath));


        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", linesPerMap);
        //job.getConfiguration().setInt("mapreduce.job.running.map.limit", maxMaps); /*Maximum simultaneous maps running*/
        // Sets reducer tasks to 1
        job.setNumReduceTasks(reducesCount);
        //job.setNumReduceTasks(1);

        //job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", linespermap);
        //job.getConfiguration().setInt("mapreduce.job.running.map.limit", maxMaps); /*Maximum simultaneous maps running*/

        String outputDir = "/user/amourao/output/" + collection + "/" + System.currentTimeMillis();
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(new Path(outputDir)))
            hdfs.delete(new Path(outputDir), true);

        boolean result = job.waitForCompletion(true);

        System.out.println("FullImageIndexer$IMAGE_COUNTERS");
        Counters cn = job.getCounters();
        CounterGroup counterGroup = cn.getGroup("FullImageIndexer$IMAGE_COUNTERS");
        for (Counter c : counterGroup) {
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        System.out.println("FullImageIndexer$PAGE_COUNTERS");
        counterGroup = cn.getGroup("FullImageIndexer$PAGE_COUNTERS");
        for (Counter c : counterGroup) {
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        System.out.println("FullImageIndexer$REDUCE_COUNTERS");
        counterGroup = cn.getGroup("FullImageIndexer$REDUCE_COUNTERS");
        for (Counter c : counterGroup) {
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        System.exit(result ? 0 : 1);
    }
}