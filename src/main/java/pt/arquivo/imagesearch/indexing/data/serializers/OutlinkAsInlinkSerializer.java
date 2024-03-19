package pt.arquivo.imagesearch.indexing.data.serializers;

import java.lang.reflect.Type;
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
        public Set<Outlink> inlinks;

        public SetOutlink(Set<Outlink> inlinks) {
            this.inlinks = inlinks;
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
        JsonArray array = new JsonArray();

        String target = inlinks.inlinks.iterator().next().getSurt();

        for (Outlink inlink: inlinks.inlinks) {
            JsonObject jsonOutlink = new JsonObject();
            jsonOutlink.addProperty("date", inlink.getCaptureDateStart().toString());
            //jsonOutlink.addProperty("end", inlink.getCaptureDateEnd().toString());
            jsonOutlink.addProperty("source", inlink.getSource());
            jsonOutlink.addProperty("anchor", inlink.getAnchor());
            array.add(jsonOutlink);
        }

        JsonObject output = new JsonObject();
        output.addProperty("url", target);
        output.addProperty("count", inlinks.inlinks.size());
        output.add("inlinks", array);

        return output;
    }
    
}
