package pt.arquivo.imagesearch.indexing.utils;

import com.j256.simplemagic.ContentInfo;
import com.j256.simplemagic.ContentInfoUtil;
import pt.arquivo.imagesearch.indexing.data.ImageData;
import org.apache.log4j.Logger;
import org.archive.url.SURT;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import java.awt.*;
import java.io.ByteArrayInputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class WARCInformationParser {
    public static final String PATTERN = "yyyyMMddHHmmss";
    private static final String TRANSFER_ENCODING_KEY = "Transfer-Encoding";

    public static Logger logger = Logger.getLogger(WARCInformationParser.class);

    public static ContentInfoUtil util = new ContentInfoUtil();

    public static LocalDateTime parseLocalDateTime(String timestamp) {
        if (timestamp.length() == WARCInformationParser.PATTERN.length() - 2)
            timestamp += "00";
        return LocalDateTime.parse(timestamp, DateTimeFormatter.ofPattern(WARCInformationParser.PATTERN));
    }

    public static String toSURT(String url) {
        if (url.startsWith("data:") || url.startsWith("hash:"))
            return url;


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

        return SURT.toSURT(url);
    }

    /*
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
     */

    public static Dimension getImageDimensions(ImageData img) {
        Dimension result = null;

        Iterator<ImageReader> iter = ImageIO.getImageReadersByMIMEType(img.getMimeDetected());

        if (!iter.hasNext()) {
            iter = ImageIO.getImageReadersByMIMEType(img.getMimeReported());
            if (iter.hasNext())
                img.setMimeDetected(img.getMimeReported());
        }

        if (!iter.hasNext()) {
            logger.debug("No reader found for given format: " + img.getMimeReported() + " " + img.getMimeDetected() + " " + img.getURLWithTimestamp());
            return null;
        }

        while (iter.hasNext()) {
            ImageReader reader = iter.next();
            try {
                ImageInputStream stream = ImageIO.createImageInputStream(new ByteArrayInputStream(img.getBytes()));
                reader.setInput(stream);
                int width = reader.getWidth(reader.getMinIndex());
                int height = reader.getHeight(reader.getMinIndex());
                return new Dimension(width, height);
            } catch (Exception e) {
                logger.error(e.getMessage() + " reader: " + reader.toString() + " " + img.getURLWithTimestamp());
            } finally {
                reader.dispose();
            }
        }

        return null;
    }

    public static String getMimeType(byte[] contentBytes) {

        ContentInfo info = util.findMatch(contentBytes);
        if (info == null)
            return null;

        String detectedMimeType = info.getMimeType();

        //Image IO is dumb and does not recognize 'image/x-ms-bmp' as 'bmp'
        if (detectedMimeType.equals("image/x-ms-bmp"))
            detectedMimeType = "image/bmp";


        return detectedMimeType;
    }

    public static void main(String[] args) {

        //TODO: check SURT and abs:src transforms
        String[] urls = {
                "https://archive.org/goo/?a=2&b&a=1",
                "http://archive.org/goo/?a=2&b&a=1",
                "http://www.archive.org/goo/?a=2&b&a=1",
                "ftp://www.archive.org/goo/?a=2&b&a=1",
        };
        for (String url : urls)
            System.out.println(WARCInformationParser.toSURT(url));
    }
}
