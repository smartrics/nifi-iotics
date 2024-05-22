package smartrics.iotics.nifi.processors.objects;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.iotics.api.SearchResponse;
import com.iotics.api.UpsertFeedWithMeta;
import com.iotics.api.UpsertInputWithMeta;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static smartrics.iotics.nifi.processors.objects.MyProperty.factory;

public class Port {

    private static final Gson gson = new GsonBuilder()
            .registerTypeAdapter(Port.class, new PortDeserializer())
            .create();

    private final String id;
    private final List<MyProperty> properties;
    private final boolean storeLast;
    private final List<MyValue> values;

    public Port(String id, List<MyProperty> properties, List<MyValue> values, boolean storeLast) {
        this.id = Objects.requireNonNull(id);
        this.properties = Optional.ofNullable(properties).orElse(List.of());
        this.storeLast = storeLast;
        this.values = Optional.ofNullable(values).orElse(List.of());
    }

    public Port(SearchResponse.FeedDetails feedDetails) {
        this(feedDetails.getFeedId().getId(),
                feedDetails.getPropertiesList().stream().map(MyProperty::factory).toList(),
                List.of(),
                feedDetails.getStoreLast());
    }

    public Port(SearchResponse.InputDetails inputDetails) {
        this(inputDetails.getInputId().getId(),
                inputDetails.getPropertiesList().stream().map(MyProperty::factory).toList(),
                List.of(),
                false);
    }

    public static UpsertFeedWithMeta feedFactory(Port port) {
        UpsertFeedWithMeta.Builder requestBuilder = UpsertFeedWithMeta.newBuilder();
        requestBuilder
                .setId(port.id)
                .setStoreLast(port.storeLast);
        port.properties.forEach(p -> requestBuilder.addProperties(factory(p)));
        port.values.forEach(v -> requestBuilder.addValues(factory(v)));
        return requestBuilder.build();
    }

    public static UpsertInputWithMeta inputsFactory(Port port) {
        UpsertInputWithMeta.Builder requestBuilder = UpsertInputWithMeta.newBuilder();
        requestBuilder.setId(port.id);
        port.properties.forEach(p -> requestBuilder.addProperties(factory(p)));
        port.values.forEach(v -> requestBuilder.addValues(factory(v)));
        return requestBuilder.build();
    }

    public static Port fromJson(String s) {
        return gson.fromJson(s, Port.class);
    }

    public String id() {
        return id;
    }

    public List<MyValue> values() {
        return values;
    }

    public List<MyProperty> properties() {
        return properties;
    }

    public boolean storeLast() {
        return storeLast;
    }

    /**
     * sets the actual values to be shared
     *
     * @param values the kvp mapping values labels to their content to be shared
     */
    public synchronized void setShares(Map<String, String> values) {
        values.forEach((key, value) -> values().stream().filter(p -> p.label().equals(key)).findFirst().ifPresent(v -> v.updateValue(value)));
    }

    public JsonObject valuesAsJson() {
        Map<String, String> map = values().stream()
                .collect(Collectors.toMap(MyValue::label, MyValue::value));
        return new Gson().toJsonTree(map).getAsJsonObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Port port = (Port) o;
        return storeLast == port.storeLast && Objects.equals(id, port.id) && Objects.equals(properties, port.properties) && Objects.equals(values, port.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, properties, storeLast, values);
    }


    @Override
    public String toString() {
        return "Port{" +
                "id='" + id + '\'' +
                '}';
    }
}
