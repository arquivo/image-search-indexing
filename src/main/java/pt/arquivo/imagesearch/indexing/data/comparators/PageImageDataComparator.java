package pt.arquivo.imagesearch.indexing.data.comparators;

import pt.arquivo.imagesearch.indexing.data.PageImageData;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Compare PageImageData, according to their image metadatas
 */
public class PageImageDataComparator implements Comparator<PageImageData>, Serializable {

    /**
     * Compares the PageImageData objects
     *
     * @param o1 first page image data to be compared.
     * @param o2 second page  image data to be compared.
     * @return a negative integer, zero, or a positive integer as the first argument is less than, equal to, or greater than the second.
     */
    @Override
    public int compare(PageImageData o1, PageImageData o2) {
        return o1.getImageMetadata().compareTo(o2.getImageMetadata());
    }
}
