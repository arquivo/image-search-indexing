package pt.arquivo.imagesearch.indexing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;

import pt.arquivo.imagesearch.indexing.data.FullImageMetadata;
import pt.arquivo.imagesearch.indexing.data.hadoop.ArchiveFileInputFormat;
import pt.arquivo.imagesearch.indexing.utils.WarcPathFilter;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static pt.arquivo.imagesearch.indexing.DupDigestMergerJob.LEGACY_MODE_STRING;

public class FullImageIndexerJob<fileList> {

    public static List<Path> getAllFilePath(Path filePath, FileSystem fs) throws FileNotFoundException, IOException {
        WarcPathFilter warcPathFilter = new WarcPathFilter();
        List<Path> fileList = new ArrayList<Path>();
        FileStatus[] fileStatus = fs.listStatus(filePath);
        for (FileStatus fileStat : fileStatus) {
            if (fileStat.isDir()) {
                fileList.addAll(getAllFilePath(fileStat.getPath(), fs));
            } else if (warcPathFilter.accept(fileStat.getPath())) {
                fileList.add(fileStat.getPath());
            }
        }
        return fileList;
    }

    public static void main(String[] args) throws Exception {
        assert args.length >= 1 : "Missing hdfs file with all arcs path argument";
        String hdfsArcsPath = args[0];

        assert args.length >= 2 : "Missing collection name argument";
        String collection = args[1];
        String jobName = collection + "_ImageIndexerWithDups";

        assert args.length >= 3 : "Missing number of warcs per map";
        int linesPerMap = Integer.parseInt(args[2]);

        assert args.length >= 4 : "Missing number of reduces";
        int reducesCount = Integer.parseInt(args[3]);

        assert args.length >= 5 : "Missing modeIsHDFS";
        boolean modeIsHDFS = Boolean.parseBoolean(args[4]);

        assert args.length >= 6 : "Missing Output mode (e.g. legacy, new)";
        String outputModeString = args[5];


        Configuration conf = new Configuration();
        conf.set("collection", collection);
        conf.set(LEGACY_MODE_STRING, outputModeString);


        Job job = Job.getInstance(conf);
        job.setJarByClass(FullImageIndexerJob.class);

        if (modeIsHDFS){
            job.setMapperClass(HDFSImageIndexerWithDupsJob.Map.class);
            job.setInputFormatClass(ArchiveFileInputFormat.class);
            // Find ArcFiles to Process
            FileSystem dfs = DistributedFileSystem.get(conf);

            RemoteIterator<LocatedFileStatus> fileIterator = dfs.listFiles(new Path(hdfsArcsPath), true);
            WarcPathFilter warcPathFilter = new WarcPathFilter();

            while (fileIterator.hasNext()) {
                LocatedFileStatus fileStatus = fileIterator.next();
                if (fileStatus.isFile() && warcPathFilter.accept(fileStatus.getPath())) {
                    ArchiveFileInputFormat.addInputPath(job, fileStatus.getPath());
                }
            }

            jobName += "HDFS";
        } else {
            job.setMapperClass(ImageIndexerWithDupsJob.Map.class);
            job.setInputFormatClass(NLineInputFormat.class);
            NLineInputFormat.addInputPath(job, new Path(hdfsArcsPath));
            job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", linesPerMap);
        }

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FullImageMetadata.class);

        job.setReducerClass(ImageIndexerWithDupsJob.Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FullImageMetadata.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setJobName(jobName);

        //job.getConfiguration().setInt("mapreduce.job.running.map.limit", 80);
        job.getConfiguration().setInt("mapreduce.map.maxattempts", 6);
        job.getConfiguration().setInt("mapreduce.reduce.shuffle.parallelcopies", 10);
        job.getConfiguration().setInt("mapreduce.task.timeout", 5400000);

        // Sets reducer tasks to 1
        job.setNumReduceTasks(reducesCount);
        //job.setNumReduceTasks(1);

        //job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", linespermap);
        //job.getConfiguration().setInt("mapreduce.job.running.map.limit", maxMaps); /*Maximum simultaneous maps running*/
        long jobTs = System.currentTimeMillis();
        String outputDirIntermediaryResults = "/user/amourao/output/" + collection + "/" + jobTs + "_dups";
        FileOutputFormat.setOutputPath(job, new Path(outputDirIntermediaryResults));

        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(new Path(outputDirIntermediaryResults)))
            hdfs.delete(new Path(outputDirIntermediaryResults), true);

        boolean result = job.waitForCompletion(true);


        System.out.println("ImageIndexerWithDupsJob$IMAGE_COUNTERS");
        Counters cn = job.getCounters();
        CounterGroup counterGroup = cn.getGroup("pt.arquivo.imagesearch.indexing.ImageIndexerWithDupsJob$IMAGE_COUNTERS");
        for (Counter c : counterGroup) {
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        System.out.println("ImageIndexerWithDupsJob$PAGE_COUNTERS");
        counterGroup = cn.getGroup("pt.arquivo.imagesearch.indexing.ImageIndexerWithDupsJob$PAGE_COUNTERS");
        for (Counter c : counterGroup) {
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        System.out.println("ImageIndexerWithDupsJob$REDUCE_COUNTERS");
        counterGroup = cn.getGroup("pt.arquivo.imagesearch.indexing.ImageIndexerWithDupsJob$REDUCE_COUNTERS");
        for (Counter c : counterGroup) {
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        if (!result){
            System.exit(1);
        }

        System.out.println("########################################################");

        jobName = collection + "_DupDigestMergerJob";
        Job jobDigest = Job.getInstance(conf);
        jobDigest.setJarByClass(DupDigestMergerJob.class);
        jobDigest.setInputFormatClass(SequenceFileInputFormat.class);

        jobDigest.setMapperClass(DupDigestMergerJob.Map.class);
        jobDigest.setMapOutputKeyClass(Text.class);
        jobDigest.setMapOutputValueClass(FullImageMetadata.class);

        jobDigest.setReducerClass(DupDigestMergerJob.Reduce.class);
        jobDigest.setOutputKeyClass(NullWritable.class);
        jobDigest.setOutputValueClass(Text.class);
        jobDigest.setOutputFormatClass(TextOutputFormat.class);

        jobDigest.setJobName(jobName);

        jobDigest.setNumReduceTasks(reducesCount);

        String inputDirDigest = outputDirIntermediaryResults;

        KeyValueTextInputFormat.setInputDirRecursive(jobDigest, true);
        KeyValueTextInputFormat.addInputPath(jobDigest, new Path(inputDirDigest));

        String outputDirDigest = "/user/amourao/output/" + collection + "/" + jobTs + "_nodups/";
        TextOutputFormat.setOutputPath(jobDigest, new Path(outputDirDigest));
        if (hdfs.exists(new Path(outputDirDigest)))
            hdfs.delete(new Path(outputDirDigest), true);

        //job.getConfiguration().setInt("mapreduce.job.running.map.limit", maxMaps); /*Maximum simultaneous maps running*/

        //job.setNumReduceTasks(1);

        //job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", linespermap);
        //job.getConfiguration().setInt("mapreduce.job.running.map.limit", maxMaps); /*Maximum simultaneous maps running*/

        result = jobDigest.waitForCompletion(true);

        System.out.println("ImageIndexerWithDupsJob$IMAGE_COUNTERS");
        cn = job.getCounters();
        counterGroup = cn.getGroup("pt.arquivo.imagesearch.indexing.ImageIndexerWithDupsJob$IMAGE_COUNTERS");
        for (Counter c : counterGroup) {
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        System.out.println("ImageIndexerWithDupsJob$PAGE_COUNTERS");
        counterGroup = cn.getGroup("pt.arquivo.imagesearch.indexing.ImageIndexerWithDupsJob$PAGE_COUNTERS");
        for (Counter c : counterGroup) {
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        System.out.println("ImageIndexerWithDupsJob$REDUCE_COUNTERS");
        counterGroup = cn.getGroup("pt.arquivo.imagesearch.indexing.ImageIndexerWithDupsJob$REDUCE_COUNTERS");
        for (Counter c : counterGroup) {
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        System.out.println("DupDigestMergerJob$COUNTERS");
        cn = jobDigest.getCounters();
        counterGroup = cn.getGroup("pt.arquivo.imagesearch.indexing.DupDigestMergerJob$COUNTERS");
        for (Counter c : counterGroup) {
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        System.out.println("DupDigestMergerJob$REDUCE_COUNTERS");
        cn = jobDigest.getCounters();
        counterGroup = cn.getGroup("pt.arquivo.imagesearch.indexing.DupDigestMergerJob$REDUCE_COUNTERS");
        for (Counter c : counterGroup) {
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        if (hdfs.exists(new Path(outputDirIntermediaryResults)))
            hdfs.delete(new Path(outputDirIntermediaryResults), true);

        System.exit(result ? 0 : 1);
    }
}