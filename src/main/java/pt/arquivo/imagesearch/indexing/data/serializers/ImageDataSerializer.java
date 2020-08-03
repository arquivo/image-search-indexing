package pt.arquivo.imagesearch.indexing.data.serializers;

import java.time.LocalDateTime;
import java.util.Base64;
import com.google.gson.*;
import pt.arquivo.imagesearch.indexing.data.ImageData;
import pt.arquivo.imagesearch.indexing.utils.ImageSearchIndexingUtil;

import java.lang.reflect.Type;
import java.util.List;

public class ImageDataSerializer implements JsonSerializer<ImageData> {

    @Override
    public JsonElement serialize(ImageData src, Type typeOfSrc, JsonSerializationContext context) {
        JsonObject obj = new JsonObject();
        obj.addProperty("imgDigest", src.getContentHash());
        obj.addProperty("type", "image");
        obj.addProperty("id", src.getId());

        obj.addProperty("oldestSurt", src.getOldestSurt());
        //obj.addProperty("oldestSurtDate", src.getOldestSurtDate().toString());

        obj.addProperty("imgSrcURLDigest", src.getImageURLHash());
        List<String> tss = ImageSearchIndexingUtil.getTimestampStandardFormat(src.getTimestamp());
        obj.add("imgTstamps", context.serialize(tss));
        obj.addProperty("imgTimespan", src.getTimespan());
        obj.addProperty("imgSrc", src.getUrl());
        obj.addProperty("imgSurt", src.getSurt());
        obj.addProperty("imgMimeTypeReported", src.getMimeReported());
        obj.addProperty("imgMimeType", src.getMimeDetected());
        obj.addProperty("imgSrcBase64", Base64.getEncoder().encodeToString(src.getBytes()));
        obj.addProperty("collection", src.getCollection());
        obj.addProperty("imgWidth", src.getWidth());
        obj.addProperty("imgHeight", src.getHeight());
        obj.addProperty("size", src.getSize());
        obj.addProperty("safe", 0);
        obj.addProperty("spam", 0);
        obj.addProperty("blocked", 0);
        obj.addProperty("warcName", src.getWarc());
        obj.addProperty("warcOffset", src.getWarcOffset());

        return obj;
    }
}
