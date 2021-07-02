package pt.arquivo.imagesearch.indexing.utils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * Class used to find only ARCs and WARCs when selecting files from HDFS
 */
public class WarcPathFilter implements PathFilter {

    @Override
    public boolean accept(Path path) {
        return path.getName().matches(".*arc\\.gz$");
    }
}
