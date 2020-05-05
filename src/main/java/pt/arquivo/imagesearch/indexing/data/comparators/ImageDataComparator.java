package pt.arquivo.imagesearch.indexing.data.comparators;

import pt.arquivo.imagesearch.indexing.data.ImageData;

import java.io.Serializable;
import java.util.Comparator;

public class ImageDataComparator implements Comparator<ImageData>, Serializable {

    @Override
    public int compare(ImageData o1, ImageData o2) {
        return o1.getContentHash().compareTo(o2.getContentHash());
    }
}
