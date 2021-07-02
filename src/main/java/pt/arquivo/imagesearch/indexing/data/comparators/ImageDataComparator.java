package pt.arquivo.imagesearch.indexing.data.comparators;

import pt.arquivo.imagesearch.indexing.data.ImageData;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Compare ImageDatas, according to their hashes
 */
public class ImageDataComparator implements Comparator<ImageData>, Serializable {

    /**
     * Compares the ImageData objects
     *
     * @param o1 first image data to be compared.
     * @param o2 second image data to be compared.
     * @return a negative integer, zero, or a positive integer as the first argument is less than, equal to, or greater than the second.
     */
    @Override
    public int compare(ImageData o1, ImageData o2) {
        return o1.getContentHash().compareTo(o2.getContentHash());
    }
}

