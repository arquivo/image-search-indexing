package pt.arquivo.imagesearch.indexing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;


public class FullDocumentIndexerJob {

    /**
     * Runs the full indexing process for the desired input
     *
     * @param args args[0]: HDFS file with (W)ARC file list, args[1]: collection name, args[2]: (W)ARC files per map, args[3]: number of reduces, args[4]: are (W)ARCs in HDFS (true, false), args[5]: are (W)ARCs in HDFS (true, false), args[5]: output mode for the JSON format (FULL, COMPACT), args[6]: temporary directory where warc files are downloaded to
     * @throws Exception crash if there is an error getting required files from HDFS
     */
    public static void main(String[] args) throws Exception {
        assert args.length >= 1 : "Missing hdfs file with all arcs path argument";
        String hdfsArcsPath = args[0];

        assert args.length >= 2 : "Missing collection name argument";
        String collection = args[1];

        assert args.length >= 3 : "Missing number of warcs per map";
        int linesPerMap = Integer.parseInt(args[2]);

        assert args.length >= 4 : "Missing number of reduces";
        int reducesCount = Integer.parseInt(args[3]);

        assert args.length >= 5 : "Missing modeIsHDFS";
        boolean modeIsHDFS = Boolean.parseBoolean(args[4]);

        assert args.length >= 7 : "Missing warcFileTempBaseDir";
        String warcFileTempBaseDir = args[6];

        long currentTime = System.currentTimeMillis();
        // the output dir for the first Hadoop job is the input dir for the second job
        String outputDirJob1 = "/document-search-indexing/output/" + collection + "/" + currentTime;

        String[] argsJob1 = new String[]{args[0], args[1], args[2], args[3], args[4], outputDirJob1, args[6]};

        int exitCode = ToolRunner.run(new DocumentIndexerWithDupsJob(), argsJob1);

        System.exit(exitCode);


    }
}