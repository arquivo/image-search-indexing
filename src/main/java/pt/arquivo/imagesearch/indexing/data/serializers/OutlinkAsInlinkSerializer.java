package pt.arquivo.imagesearch.indexing.data.serializers;

import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.Set;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import pt.arquivo.imagesearch.indexing.data.Outlink;

public class OutlinkAsInlinkSerializer implements JsonSerializer<OutlinkAsInlinkSerializer.SetOutlink> {

    // Serializer for Outlink objects doesn't like generic types (Set<Outlink>), so we need to wrap it in a class
    public static class SetOutlink {
        public Set<Outlink> inlinksInternal;
        public Set<Outlink> inlinksExternal;

        public SetOutlink(Set<Outlink> inlinksInternal, Set<Outlink> inlinksExternal) {
            this.inlinksInternal = inlinksInternal;
            this.inlinksExternal = inlinksExternal;
        }
    }

    /**
     * Converts the object into a JSON ready for writing
     *
     * @param src object to export
     * @param typeOfSrc (unused)
     * @param context Hadoop context
     * @return JsonElement ready for export
     */
    @Override
    public JsonElement serialize(SetOutlink inlinks, Type typeOfSrc, JsonSerializationContext context) {

        String target = inlinks.inlinksInternal.iterator().next().getSurt();

        JsonObject output = new JsonObject();

        JsonArray array = new JsonArray();

        Set<Outlink> inlinksAll = new HashSet<>();
        inlinksAll.addAll(inlinks.inlinksInternal);
        inlinksAll.addAll(inlinks.inlinksExternal);


        for (Outlink inlink: inlinksAll) {
            JsonObject jsonOutlink = new JsonObject();
            jsonOutlink.addProperty("date", inlink.getCaptureDateStart().toString());
            //jsonOutlink.addProperty("end", inlink.getCaptureDateEnd().toString());
            jsonOutlink.addProperty("source", inlink.getSource());
            jsonOutlink.addProperty("anchor", inlink.getAnchor());
            array.add(jsonOutlink);
        }
        output.add("inlinks", array);

        output.addProperty("url", target);
        output.addProperty("count", inlinksAll.size());
        output.addProperty("countInternal", inlinks.inlinksInternal.size());
        output.addProperty("countExternal", inlinks.inlinksExternal.size());
        

        return output;
    }
    
}
