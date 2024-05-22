package smartrics.iotics.nifi.processors.objects;

import com.github.jsonldjava.shaded.com.google.common.reflect.TypeToken;
import com.google.gson.*;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Optional;

public class PortDeserializer implements JsonDeserializer<Port> {

    @Override
    public Port deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        JsonObject jsonObject = json.getAsJsonObject();

        String id = Optional.ofNullable(jsonObject.get("id")).map(JsonElement::getAsString).orElse(null);
        boolean storeLast = Optional.ofNullable(jsonObject.get("storeLast")).map(JsonElement::getAsBoolean).orElse(false);

        List<MyProperty> properties = (List<MyProperty>) Optional.ofNullable(jsonObject.get("properties"))
                .filter(JsonElement::isJsonArray)
                .map(e -> context.deserialize(e, new TypeToken<List<MyProperty>>(){}.getType()))
                .orElse(List.of());

        List<MyValue> values = (List<MyValue>) Optional.ofNullable(jsonObject.get("values"))
                .filter(JsonElement::isJsonArray)
                .map(e -> context.deserialize(e, new TypeToken<List<MyValue>>(){}.getType()))
                .orElse(List.of());

        return new Port(id, properties, values, storeLast);
    }
}
