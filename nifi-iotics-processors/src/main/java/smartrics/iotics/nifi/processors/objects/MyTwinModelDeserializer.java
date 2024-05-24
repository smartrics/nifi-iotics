package smartrics.iotics.nifi.processors.objects;

import com.github.jsonldjava.shaded.com.google.common.reflect.TypeToken;
import com.google.gson.*;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Optional;

public class MyTwinModelDeserializer implements JsonDeserializer<MyTwinModel> {

    @Override
    public MyTwinModel deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        JsonObject jsonObject = json.getAsJsonObject();

        String hostId = Optional.ofNullable(jsonObject.get("hostId")).map(JsonElement::getAsString).orElse(null);
        String id = Optional.ofNullable(jsonObject.get("id")).map(JsonElement::getAsString).orElse(null);

        List<MyProperty> properties = (List<MyProperty>) Optional.ofNullable(jsonObject.get("properties"))
                .filter(JsonElement::isJsonArray)
                .map(e -> context.deserialize(e, new TypeToken<List<MyProperty>>(){}.getType()))
                .orElse(List.of());

        List<Port> feeds = (List<Port>) Optional.ofNullable(jsonObject.get("feeds"))
                .filter(JsonElement::isJsonArray)
                .map(e -> context.deserialize(e, new TypeToken<List<Port>>(){}.getType()))
                .orElse(List.of());

        List<Port> inputs = (List<Port>) Optional.ofNullable(jsonObject.get("inputs"))
                .filter(JsonElement::isJsonArray)
                .map(e -> context.deserialize(e, new TypeToken<List<Port>>(){}.getType()))
                .orElse(List.of());

        return new MyTwinModel(hostId, id, properties, feeds, inputs);
    }
}
