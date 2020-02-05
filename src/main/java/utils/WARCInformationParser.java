package utils;

import data.PageImageData;
import org.archive.url.SURT;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class WARCInformationParser {
    public static final String PATTERN = "yyyyMMddHHmmss";

    public static LocalDateTime parseLocalDateTime(String timestamp) {
        if (timestamp.length() == WARCInformationParser.PATTERN.length() - 2)
            timestamp += "00";
        return LocalDateTime.parse(timestamp, DateTimeFormatter.ofPattern(WARCInformationParser.PATTERN));
    }

    public static String toSURT(String url) {
        String surt = SURT.toSURT(url);
        if (surt.startsWith("https://"))
            surt = surt.substring("https://".length());
        return surt;
    }

    public static PageImageData getClosest(List<PageImageData> pages, LocalDateTime timekey) {
        pages.sort(Comparator.comparing(PageImageData::getTimestamp));
        int idx = Collections.binarySearch(pages, timekey);
        if (idx < 0) {
            idx = -idx - 1;
        }

        if (idx == 0) {
            return pages.get(idx);
        } else if (idx == pages.size()) {
            return pages.get(idx - 1);
        }

        return timekey.compareTo(pages.get(idx - 1).getTimestamp()) < timekey.compareTo(pages.get(idx).getTimestamp()) ? pages.get(idx - 1) : pages.get(idx);
    }

}
