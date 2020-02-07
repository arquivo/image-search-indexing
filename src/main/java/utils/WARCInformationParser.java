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
        if (url.startsWith("https://"))
            url = url.substring("https://".length());
        if (url.startsWith("http://"))
            url = url.substring("http://".length());
        if (url.startsWith("www."))
            url = url.substring("www.".length());
        if (url.startsWith("ww2."))
            url = url.substring("ww2.".length());
        if (url.startsWith("ww3."))
            url = url.substring("ww3.".length());

        String surt = SURT.toSURT(url);

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

    public static void main(String[] args) {

        //TODO: check SURT and abs:src transforms
        String[] urls = {
                "https://archive.org/goo/?a=2&b&a=1",
                "http://archive.org/goo/?a=2&b&a=1",
                "http://www.archive.org/goo/?a=2&b&a=1",
                "ftp://www.archive.org/goo/?a=2&b&a=1",
        };
        for(String url: urls)
            System.out.println(WARCInformationParser.toSURT(url));
    }
}
