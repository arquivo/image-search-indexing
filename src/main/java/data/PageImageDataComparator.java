package data;

import java.util.Comparator;

public class PageImageDataComparator implements Comparator<PageImageData> {

    @Override
    public int compare(PageImageData t1, PageImageData t2) {
        if (t1.getTimestamp().equals(t2)){
            return t1.getPageURL().length() - t1.getPageURL().length();
        } else {
            return t1.getTimestamp().compareTo(t2.getTimestamp());
        }
    }
}
