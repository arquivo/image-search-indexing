package pt.arquivo.imagesearch.indexing;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import pt.arquivo.imagesearch.indexing.data.FullImageMetadata;
import pt.arquivo.imagesearch.indexing.data.PageImageData;
import pt.arquivo.imagesearch.indexing.processors.ImageInformationExtractor;
import pt.arquivo.imagesearch.indexing.utils.ImageSearchIndexingUtil;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CaptionExtractTest {

    @Test
    public void extractCaptionsPageCaptionParent() throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();

        ImageInformationExtractor iie = new ImageInformationExtractor("Teste");
        URL warcURL = classLoader.getResource("pages/pageCaptionParent.html");

        assertNotNull(warcURL);

        String pageURL = "https://andremourao.com/static/pages";
        String pageTstamp = "20200101000000";

        byte[] htmlBytes = FileUtils.readFileToByteArray(new File(warcURL.getPath()));

        String html = ImageSearchIndexingUtil.decode(htmlBytes, iie);

        iie.parseHTMLPage(pageURL, pageTstamp, "", 0, html);

        HashMap<String, FullImageMetadata> entries = iie.getEntries();

        FullImageMetadata fim1 = entries.get("(pt,iol,)/multimedia/oratvi/multimedia/imagem/id/5b3e33f10cf2d0e0ad0062d6/1024");
        assertNotNull(fim1);

        PageImageData pid = fim1.getPageImageDatas().firstKey();
        assertNotNull(pid);
        assertEquals(pid.getImgCaption(), "Image 1 This is caption 1 Page 2");
        assertEquals(pid.getImgAlt(), "I'm a PNG that lies and say I'm a JPG");
        assertEquals(pid.getImgTitle(), "I also have a title");

        fim1 = entries.get("(com,andremourao,)/media/me-latin1%c3%a7%c3%a3o.jpg");
        assertNotNull(fim1);
        pid = fim1.getPageImageDatas().firstKey();
        assertNotNull(pid);
        assertEquals(pid.getImgCaption(), "This is caption 2 Page 2");
        assertEquals(pid.getImgAlt(), "");
        assertEquals(pid.getImgTitle(), "");

        fim1 = entries.get("(com,andremourao,)/wp-content/uploads/2013/11/poster-220x220.png");
        assertNotNull(fim1);
        pid = fim1.getPageImageDatas().firstKey();
        assertNotNull(pid);
        assertEquals(pid.getImgCaption(), "Image 3\nThis is caption 3 Page 2");
        assertEquals(pid.getImgAlt(), "");
        assertEquals(pid.getImgTitle(), "");



    }


    @Test
    public void extractCaptionsPageCaptionSiblings() throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();

        ImageInformationExtractor iie = new ImageInformationExtractor("Teste");
        URL warcURL = classLoader.getResource("pages/pageCaptionSiblings.html");

        assertNotNull(warcURL);

        String pageURL = "https://andremourao.com/static/pages";
        String pageTstamp = "20200101000000";

        byte[] htmlBytes = FileUtils.readFileToByteArray(new File(warcURL.getPath()));

        String html = ImageSearchIndexingUtil.decode(htmlBytes, iie);

        iie.parseHTMLPage(pageURL, pageTstamp, "", 0, html);

        HashMap<String, FullImageMetadata> entries = iie.getEntries();

        FullImageMetadata fim1 = entries.get("(pt,iol,)/multimedia/oratvi/multimedia/imagem/id/5b3e33f10cf2d0e0ad0062d6/1024");
        assertNotNull(fim1);

        PageImageData pid = fim1.getPageImageDatas().firstKey();
        assertNotNull(pid);
        assertEquals(pid.getImgCaption(), "This is image 1 Page 3\nThis is caption 1 Page 3");
        assertEquals(pid.getImgAlt(), "I'm a PNG that lies and say I'm a JPG");
        assertEquals(pid.getImgTitle(), "I also have a title");

        fim1 = entries.get("(com,andremourao,)/media/me-latin1%c3%a7%c3%a3o.jpg");
        assertNotNull(fim1);
        pid = fim1.getPageImageDatas().firstKey();
        assertNotNull(pid);
        assertEquals(pid.getImgCaption(), "This is caption 1 Page 3\nThis is caption 2 Page 3");
        assertEquals(pid.getImgAlt(), "");
        assertEquals(pid.getImgTitle(), "");

        fim1 = entries.get("(com,andremourao,)/wp-content/uploads/2013/11/poster-220x220.png");
        assertNotNull(fim1);
        pid = fim1.getPageImageDatas().firstKey();
        assertNotNull(pid);
        assertEquals(pid.getImgCaption(), "This is image 3 Page 3\nThis is caption 3 Page 3");
        assertEquals(pid.getImgAlt(), "");
        assertEquals(pid.getImgTitle(), "");



    }

    @Test
    public void extractCaptionsPageCaptionTooLarge() throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();

        ImageInformationExtractor iie = new ImageInformationExtractor("Teste");
        URL warcURL = classLoader.getResource("pages/pageCaptionTooLarge.html");

        assertNotNull(warcURL);

        String pageURL = "https://andremourao.com/static/pages";
        String pageTstamp = "20200101000000";

        byte[] htmlBytes = FileUtils.readFileToByteArray(new File(warcURL.getPath()));

        String html = ImageSearchIndexingUtil.decode(htmlBytes, iie);

        iie.parseHTMLPage(pageURL, pageTstamp, "", 0, html);

        HashMap<String, FullImageMetadata> entries = iie.getEntries();

        FullImageMetadata fim1 = entries.get("(pt,iol,)/multimedia/oratvi/multimedia/imagem/id/5b3e33f10cf2d0e0ad0062d6/1024");
        assertNotNull(fim1);

        PageImageData pid = fim1.getPageImageDatas().firstKey();
        assertNotNull(pid);

        assertEquals(pid.getImgCaption(), "This caption is too large large large large large large large large large large large large large large large large large\ntoo too too too too too too too too too too too too too too too too too too too too too too too too too too too too too too");
        assertEquals(pid.getImgAlt(), "I'm a PNG that lies and say I'm a JPG");
        assertEquals(pid.getImgTitle(), "I also have a title");

    }

}
