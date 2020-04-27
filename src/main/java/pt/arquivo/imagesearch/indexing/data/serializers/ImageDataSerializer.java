package pt.arquivo.imagesearch.indexing.data.serializers;

import java.util.Base64;
import com.google.gson.*;
import pt.arquivo.imagesearch.indexing.data.ImageData;

import java.lang.reflect.Type;

public class ImageDataSerializer implements JsonSerializer<ImageData> {

    @Override
    public JsonElement serialize(ImageData src, Type typeOfSrc, JsonSerializationContext context) {
        JsonObject obj = new JsonObject();
        obj.addProperty("type", "image");
        obj.addProperty("id", src.getId());
        obj.addProperty("imageURLHash", src.getImageURLHash());
        obj.add("timestamps", context.serialize((src.getTimestampOriginalFormat())));
        obj.addProperty("timestamp", src.getTimestampOriginalFormat().get(0));
        obj.addProperty("url", src.getUrl());
        obj.addProperty("surt", src.getSurt());
        obj.addProperty("mimeReported", src.getMimeReported());
        obj.addProperty("mime", src.getMimeDetected());
        obj.addProperty("imgSrcBase64", Base64.getEncoder().encodeToString(src.getBytes()));
        obj.addProperty("collection", src.getCollection());
        obj.addProperty("width", src.getWidth());
        obj.addProperty("height", src.getHeight());
        obj.addProperty("size", src.getSize());
        obj.addProperty("safe", 1);
        obj.addProperty("spam", 0);

        return obj;
    }
}
