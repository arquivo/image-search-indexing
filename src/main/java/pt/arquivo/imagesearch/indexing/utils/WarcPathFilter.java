package pt.arquivo.imagesearch.indexing.utils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class WarcPathFilter implements PathFilter {

    @Override
    public boolean accept(Path path) {
        return path.getName().matches(".*arc\\.gz$");
    }
}
