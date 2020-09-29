package pt.arquivo.imagesearch.indexing.data.serializers;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import pt.arquivo.imagesearch.indexing.data.FullImageMetadata;
import pt.arquivo.imagesearch.indexing.data.ImageData;
import pt.arquivo.imagesearch.indexing.data.PageImageData;
import pt.arquivo.imagesearch.indexing.utils.ImageSearchIndexingUtil;

import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class LegacyFullImageMetadataSerializer implements JsonSerializer<FullImageMetadata> {

    @Override
    public JsonElement serialize(FullImageMetadata src, Type typeOfSrc, JsonSerializationContext context) {
        if (src.getPageImageDatas().isEmpty() || src.getImageDatas().isEmpty())
            return null;

        JsonObject obj = new JsonObject();
        PageImageData pid = src.getPageImageDatas().firstKey();
        ImageData id = src.getImageDatas().firstKey();


        DateTimeFormatter FORMAT = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

        ArrayList<String> imgTstamp = new ArrayList<String>();
        imgTstamp.add(pid.getImgTimestamp().format(FORMAT));


        obj.addProperty("pageProtocol", pid.getPageProtocol());
        obj.addProperty("imgHeight", pid.getImgHeight());
        obj.addProperty("imgWidth", pid.getImgWidth());
        obj.addProperty("imgSrcTokens", pid.getImgURLTokens());

        obj.addProperty("pageTitle", pid.getPageTitle());
        obj.addProperty("pageURLTokens", pid.getPageURLTokens());
        obj.addProperty("pageTstamp", pid.getPageTimestamp().format(FORMAT));
        obj.addProperty("pageURL", pid.getPageURL());

        obj.addProperty("imgSrcURLDigest", pid.getImgURLHash());

        if (!pid.getImgTitle().isEmpty())
            obj.addProperty("imgTitle", pid.getImgTitle());
        if (!pid.getImgAlt().isEmpty())
            obj.addProperty("imgAlt", pid.getImgAlt());

        obj.addProperty("imgMimeType", pid.getImgMimeType());
        obj.addProperty("imgSrcBase64", Base64.getEncoder().encodeToString(id.getBytes()));
        obj.add("imgTstamp", context.serialize(imgTstamp));
        obj.addProperty("imgDigest", pid.getImageDigest());
        obj.addProperty("pageImages", pid.getImagesInPage());
        obj.addProperty("collection", pid.getCollection());
        obj.addProperty("imgSrc", pid.getImgURL());

        obj.addProperty("pageHost", pid.getPageHost());

        obj.addProperty("safe", 0);

        return obj;
    }
}
