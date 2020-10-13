package pt.arquivo.imagesearch.indexing.data.serializers;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import pt.arquivo.imagesearch.indexing.data.MultiPageImageData;
import pt.arquivo.imagesearch.indexing.data.PageImageData;

import java.lang.reflect.Type;

public class MultiPageImageDataSerializer implements JsonSerializer<MultiPageImageData> {

    @Override
    public JsonElement serialize(MultiPageImageData src, Type typeOfSrc, JsonSerializationContext context) {
        JsonObject obj = new JsonObject();
        obj.addProperty("imgDigest", src.getImageDigest());
        obj.addProperty("type", "page");

        obj.addProperty("id", src.getId());

        //obj.addProperty("oldestSurtDate", src.getOldestSurtDate().toString());
        if (!src.getImgTitle().isEmpty())
            obj.add("imgTitle", context.serialize(src.getImgTitle()));

        if (!src.getImgAlt().isEmpty())
            obj.add("imgAlt", context.serialize(src.getImgAlt()));

        if (!src.getImgCaption().isEmpty())
            obj.add("imgCaption", context.serialize(src.getImgCaption()));

        obj.addProperty("imgUrl", src.getImgURL());
        obj.addProperty("imgUrlDigest", src.getImgURLHash());
        obj.addProperty("imgUrlTokens", src.getImgURLTokens());

        obj.addProperty("pageTitle", src.getPageTitle());
        obj.addProperty("pageUrlTokens", src.getPageURLTokens());

        //obj.addProperty("imgId", src.getImgId());
        obj.addProperty("imgHeight", src.getImgHeight());
        obj.addProperty("imgWidth", src.getImgWidth());
        obj.addProperty("imgMimeType", src.getImgMimeType());
        obj.addProperty("imgCrawlTimespan", src.getTimespan());
        obj.addProperty("imgCrawlTimestamp", src.getImgTimestamp().toString());

        obj.addProperty("pageHost", src.getPageHost());

        obj.addProperty("pageCrawlTimestamp", src.getPageTimestamp().toString());
        obj.addProperty("pageCrawlTimespan", src.getTimespan());
        obj.addProperty("pageUrl", src.getPageURL());
        obj.addProperty("pageUrlDigest", src.getPageURLHash());

        obj.addProperty("isInline", src.getInline());
        obj.add("tagFoundIn", context.serialize(src.getTagFoundIn()));

        obj.addProperty("imagesInOriginalPage", src.getImagesInPage());
        obj.addProperty("imageMetadataChanges", src.getImageMetadataChanges());
        obj.addProperty("pageMetadataChanges", src.getPageMetadataChanges());
        obj.addProperty("matchingImages", src.getMatchingImages());
        obj.addProperty("matchingPages", src.getMatchingPages());

        obj.addProperty("collection", src.getCollection());

        obj.addProperty("safe", 0);
        obj.addProperty("spam", 0);
        obj.addProperty("blocked", 0);

        return obj;
    }
}
