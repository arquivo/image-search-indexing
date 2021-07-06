package pt.arquivo.imagesearch.indexing;

import org.apache.hadoop.util.ToolRunner;


public class FullImageIndexerJob {

    /**
     * Runs the full indexing process for the desired input
     *
     * @param args args[0]: HDFS file with (W)ARC file list, args[1]: collection name, args[2]: (W)ARC files per map, args[3]: number of reduces, args[4]: are (W)ARCs in HDFS (true, false), args[5]: are (W)ARCs in HDFS (true, false), args[5]: output mode for the JSON format (FULL, COMPACT)
     * @throws Exception crash if there is an error getting required files from HDFS
     */
    public static void main(String[] args) throws Exception {

        String[] argsJob1 = new String[]{args[0], args[1], args[2], args[3], args[4]};

        int exitCode = ToolRunner.run(new ImageIndexerWithDupsJob(), argsJob1);
        if (exitCode != 0)
            System.exit(exitCode);

        String[] argsJob2 = new String[]{args[1], args[3], args[5]};
        exitCode = ToolRunner.run(new DupDigestMergerJob(), argsJob2);
        System.exit(exitCode);

    }
}