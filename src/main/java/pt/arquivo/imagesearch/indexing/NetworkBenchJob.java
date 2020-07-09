package pt.arquivo.imagesearch.indexing;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import pt.arquivo.imagesearch.indexing.utils.ImageSearchIndexingUtil;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

public class NetworkBenchJob {


    public static class Map extends Mapper<LongWritable, Text, Text, Writable> {

        private final Logger logger = Logger.getLogger(Map.class);
        public String collection;

        public void map(LongWritable key, Text value, Context context) throws IOException {
            String arcURL = value.toString();
            if (!arcURL.isEmpty()) {
                logger.info("(W)ARCNAME: " + arcURL);

                URL url = null;
                try {
                    url = new URL(arcURL);
                } catch (MalformedURLException ignored) {

                }
                String[] surl = url.getPath().split("/");
                String arcName = surl[surl.length - 1];
                String filename = "/tmp/" + System.currentTimeMillis() + "_" + arcName;

                try {
                    int fileSize = ImageSearchIndexingUtil.getFileSize(url);
                    //ImageSearchIndexingUtil.saveFile(url, filename);
                    File dest = new File( filename);
                    FileUtils.copyURLToFile(url, dest, 1000*60, 1000*30);
                    dest = new File( filename);
                    if (fileSize != dest.length()){
                        FileUtils.deleteQuietly(dest);
                        throw new IOException("Incomplete file: " + fileSize +  " " + dest.length());
                    }
                    FileUtils.deleteQuietly(dest);
                } catch (IOException e) {
                    e.printStackTrace();
                    File dest = new File( filename);
                    FileUtils.deleteQuietly(dest);
                    throw e;
                }


            }
        }
    }

    public static void main(String[] args) throws Exception {
        assert args.length >= 1 : "Missing hdfs file with all arcs path argument";
        String hdfsArcsPath = args[0];

        assert args.length >= 2 : "Missing collection name argument";
        String collection = args[1];
        String jobName = collection + "_NetworkBenchJob";

        assert args.length >= 3 : "Missing number of warcs per map";
        int linesPerMap = Integer.parseInt(args[2]);

        Configuration conf = new Configuration();
        conf.set("collection", collection);

        Job job = Job.getInstance(conf);
        job.setJarByClass(NetworkBenchJob.class);
        job.setInputFormatClass(NLineInputFormat.class);

        job.setMapperClass(NetworkBenchJob.Map.class);

        job.setJobName(jobName);

        NLineInputFormat.addInputPath(job, new Path(hdfsArcsPath));

        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", linesPerMap);
        job.getConfiguration().setFloat("mapreduce.job.reduce.slowstart.completedmaps", 0.9f);

        //job.getConfiguration().setInt("mapreduce.job.running.map.limit", maxMaps); /*Maximum simultaneous maps running*/
        // Sets reducer tasks to 1
        job.setNumReduceTasks(0);

        FileOutputFormat.setOutputPath(job, new Path("/dev/null"));

        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(new Path("/dev/null")))
            hdfs.delete(new Path("/dev/null"), true);

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}