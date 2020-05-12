package pt.arquivo.imagesearch.indexing.data.comparators;

import pt.arquivo.imagesearch.indexing.data.PageImageData;

import java.io.Serializable;
import java.util.Comparator;

public class PageImageDataComparator implements Comparator<PageImageData>, Serializable {

    @Override
    public int compare(PageImageData o1, PageImageData o2) {
        int diff1 = o1.getImageMetadata().compareTo(o2.getImageMetadata());
        return diff1;
        //if (diff1 != 0)
        //    return diff1;
        //return o1.getPageMetadata().compareTo(o2.getPageMetadata());
    }
}
