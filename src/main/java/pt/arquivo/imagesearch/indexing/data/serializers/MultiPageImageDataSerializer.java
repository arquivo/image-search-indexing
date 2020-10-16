package pt.arquivo.imagesearch.indexing.data.serializers;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import pt.arquivo.imagesearch.indexing.data.MultiPageImageData;

import java.lang.reflect.Type;

public class MultiPageImageDataSerializer implements JsonSerializer<MultiPageImageData> {

    @Override
    public JsonElement serialize(MultiPageImageData src, Type typeOfSrc, JsonSerializationContext context) {
        JsonObject obj = new JsonObject();
        obj.addProperty("type", "page");

        obj.addProperty("id", src.getImageDigest());

        //obj.addProperty("oldestSurtDate", src.getOldestSurtDate().toString());
        if (!src.getImgTitle().isEmpty())
            obj.add("imgTitle", context.serialize(src.getImgTitle()));

        if (!src.getImgAlt().isEmpty())
            obj.add("imgAlt", context.serialize(src.getImgAlt()));

        if (!src.getImgCaption().isEmpty())
            obj.add("imgCaption", context.serialize(src.getImgCaption()));

        obj.addProperty("imgUrl", src.getImgURL());
        obj.addProperty("imgUrlTokens", src.getImgURLTokens());

        obj.addProperty("pageTitle", src.getPageTitle());
        obj.addProperty("pageUrlTokens", src.getPageURLTokens());

        //obj.addProperty("imgId", src.getImgId());
        obj.addProperty("imgHeight", src.getImgHeight());
        obj.addProperty("imgWidth", src.getImgWidth());
        obj.addProperty("imgMimeType", src.getImgMimeType());
        obj.addProperty("imgCrawlTimestampLatest", src.getLatastTimestamp().toString());
        obj.addProperty("imgCrawlTimestamp", src.getImgTimestamp().toString());

        obj.addProperty("pageHost", src.getPageHost());

        obj.addProperty("pageCrawlTimestamp", src.getPageTimestamp().toString());
        obj.addProperty("pageUrl", src.getPageURL());

        obj.addProperty("isInline", src.getInline());
        obj.add("tagFoundIn", context.serialize(src.getTagFoundIn()));

        obj.addProperty("imagesInOriginalPage", src.getImagesInPage());
        obj.addProperty("imageMetadataChanges", src.getImageMetadataChanges());
        obj.addProperty("pageMetadataChanges", src.getPageMetadataChanges());
        obj.addProperty("matchingImages", src.getMatchingImages());
        obj.addProperty("matchingPages", src.getMatchingPages());

        obj.add("collection", context.serialize(new String[]{src.getCollection()}));

        obj.addProperty("safe", 0);
        obj.addProperty("spam", 0);
        obj.addProperty("blocked", 0);

        return obj;
    }
}
