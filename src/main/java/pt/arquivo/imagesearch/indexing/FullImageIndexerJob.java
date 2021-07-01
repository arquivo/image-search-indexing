package pt.arquivo.imagesearch.indexing;

import org.apache.hadoop.util.ToolRunner;


public class FullImageIndexerJob {

    public static void main(String[] args) throws Exception {

        String[] argsJob1 = new String[]{args[0], args[1], args[2], args[3], args[4]};

        int exitCode = ToolRunner.run(new ImageIndexerWithDupsJob(), argsJob1);
        if (exitCode != 0)
            System.exit(exitCode);

        String[] argsJob2 = new String[]{args[1], args[2], args[5]};
        exitCode = ToolRunner.run(new DupDigestMergerJob(), argsJob2);
        System.exit(exitCode);

    }
}