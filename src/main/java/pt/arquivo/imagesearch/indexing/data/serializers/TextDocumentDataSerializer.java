package pt.arquivo.imagesearch.indexing.data.serializers;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import pt.arquivo.imagesearch.indexing.data.MultiPageImageData;
import pt.arquivo.imagesearch.indexing.data.TextDocumentData;
import pt.arquivo.imagesearch.indexing.processors.ImageInformationExtractor;
import pt.arquivo.imagesearch.indexing.utils.ImageSearchIndexingUtil;
import pt.arquivo.imagesearch.indexing.utils.WARCInformationParser;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;

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
        obj.addProperty("type", "document");

        obj.addProperty("id", src.getId());

        obj.addProperty("title", src.getTitle());
        obj.addProperty("url", src.getUrl());
        obj.addProperty("urlTokens", src.getUrlTokens());
        obj.addProperty("host", src.getHost());
        obj.addProperty("timestamp", src.getTimestampString());
        obj.addProperty("mimeTypeDetected", src.getMimeTypeDetected());
        obj.addProperty("mimeTypeReported", src.getMimeTypeReported());
        obj.addProperty("content", src.getContent());
        obj.addProperty("encodingDetected", src.getEncodingDetected());
        obj.addProperty("encodingReported", src.getEncodingReported());
        obj.addProperty("warc", src.getWarc());
        obj.addProperty("warcOffset", src.getWarcOffset());
        obj.addProperty("surt", src.getSurt());
        obj.addProperty("protocol", src.getProtocol());
        obj.addProperty("urlHash", src.getURLHash());
        obj.addProperty("collection", src.getCollection());
        obj.add("outlinks", context.serialize(src.getOutlinks()));
        
        obj.addProperty("safe", 0);
        obj.addProperty("spam", 0);
        obj.addProperty("blocked", 0);

        return obj;
    }
}
