package pt.arquivo.imagesearch.indexing.data.serializers;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import pt.arquivo.imagesearch.indexing.data.TextDocumentData;
import java.lang.reflect.Type;

/**
 * Configures JsonSerializable to export the object in the desired JSON format
 * Used in the COMPACT export format
 */
public class TextDocumentDataSerializer implements JsonSerializer<TextDocumentData> {

    /**
     * Converts the object into a JSON ready for writing
     *
     * @param src object to export
     * @param typeOfSrc (unused)
     * @param context Hadoop context
     * @return JsonElement ready for export
     */
    @Override
    public JsonElement serialize(TextDocumentData src, Type typeOfSrc, JsonSerializationContext context) {
        JsonObject obj = new JsonObject();
        //obj.addProperty("type", "document");

        obj.addProperty("id", src.getId());

        obj.addProperty("digestContainer", src.getDigestContainer());
        obj.add("urls", context.serialize(src.getURL()));
        obj.addProperty("date", src.getTimestampFormatted());
        obj.addProperty("tstamp", src.getTimestampString());
        if (src.getTitle() != null && !src.getTitle().isEmpty())
            obj.add("title", context.serialize(src.getTitle()));
        obj.addProperty("type", src.getMimeTypeDetected());
        obj.addProperty("typeReported", src.getMimeTypeReported());
        if (src.getContent() != null && !src.getContent().isEmpty())
            obj.add("content", context.serialize(src.getContent()));
        obj.add("urlTokens", context.serialize(src.getURLTokens()));
        obj.add("host", context.serialize(src.getHost()));
        if (src.getMetadata() != null && !src.getMetadata().isEmpty())
            obj.add("metadata", context.serialize(src.getMetadata()));


        //obj.addProperty("warc", src.getWarc());
        //obj.addProperty("warcOffset", src.getWarcOffset());
        //obj.addProperty("surt", src.getSurt());
        //obj.addProperty("protocol", src.getProtocol());
        //obj.addProperty("urlHash", src.getURLHash());
        obj.add("collection", context.serialize(src.getCollection()));
        //obj.add("outlinks", context.serialize(src.getOutlinks()));
        obj.add("inlinks", context.serialize(src.getInlinkSurts().size()));
        obj.add("inlinkAnchors", context.serialize(src.getInlinkAnchors()));

        
        //obj.addProperty("safe", 0);
        //obj.addProperty("spam", 0);
        //obj.addProperty("blocked", 0);

        return obj;
    }
}
