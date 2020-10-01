package pt.arquivo.imagesearch.indexing.data.serializers;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import pt.arquivo.imagesearch.indexing.data.MultiPageImageData;
import pt.arquivo.imagesearch.indexing.data.PageImageData;

import java.lang.reflect.Type;

public class MultiPageImageDataSerializer implements JsonSerializer<MultiPageImageData> {

    /*
    private String imgTitle;
    private String imgAlt;
    private String imgFilename;
    private String imgCaption;

    private String pageTitle;
    private String pageURLTokens;

    private String imgURL;
    private String imgURLTokens;
    private String imgSurt;

    private String pageTimestamp;
    private String pageURL;
    private String pageURLHash;

    private String pageHost;
    private String pageProtocol;

    private LocalDateTime timestamp;

    // Number of images in the original page
    private int imagesInPage;

    // Total number of matching <img src="">
    private int imgReferencesInPage;

    private boolean isInline;

    private Set<String> tagFoundIn;
     */
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

        obj.addProperty("imgSrc", src.getImgURL());
        obj.addProperty("imgSrcURLDigest", src.getImgURLHash());

        obj.addProperty("imgSrcTokens", src.getImgURLTokens());

        obj.addProperty("pageTitle", src.getPageTitle());
        obj.addProperty("pageURLTokens", src.getPageURLTokens());

        obj.addProperty("imgId", src.getImgId());
        obj.addProperty("imgTstamp", src.getImgTimestamp().toString());
        obj.addProperty("imgHeight", src.getImgHeight());
        obj.addProperty("imgWidth", src.getImgWidth());
        obj.addProperty("imgMimeType", src.getImgMimeType());
        obj.addProperty("imgTimespan", src.getTimespan());

        obj.addProperty("pageHost", src.getPageHost());

        obj.addProperty("pageTstamp", src.getPageTimestamp().toString());
        obj.addProperty("pageTimespan", src.getTimespan());
        obj.addProperty("pageURL", src.getPageURL());
        obj.addProperty("pageURLHash", src.getPageURLHash());

        obj.addProperty("isInline", src.getInline());
        obj.add("tagFoundIn", context.serialize(src.getTagFoundIn()));

        obj.addProperty("imagesInOriginalPage", src.getImagesInPage());
        obj.addProperty("imageMetadataChanges", src.getImageMetadataChanges());
        obj.addProperty("pageMetadataChanges", src.getPageMetadataChanges());
        obj.addProperty("matchingImages", src.getMatchingImages());
        obj.addProperty("matchingPages", src.getMatchingPages());

        obj.addProperty("imgWarcName", src.getImgWarc());
        obj.addProperty("imgWarcOffset", src.getImgWarcOffset());
        obj.addProperty("collection", src.getCollection());

        obj.addProperty("safe", 0);
        obj.addProperty("spam", 0);
        obj.addProperty("blocked", 0);

        return obj;
    }
}
