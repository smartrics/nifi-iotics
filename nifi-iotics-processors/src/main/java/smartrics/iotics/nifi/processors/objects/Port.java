package smartrics.iotics.nifi.processors.objects;

import com.google.gson.Gson;
import com.iotics.api.SearchResponse;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class Port {

    private final String id;
    private final List<MyProperty> properties;
    private final boolean storeLast;
    private Map<String, Object> payload;

    public Port(String id, List<MyProperty> properties, boolean storeLast, Map<String, Object> payload) {
        this.id = Objects.requireNonNull(id);
        this.properties = Optional.ofNullable(properties).orElse(List.of());
        this.storeLast = storeLast;
        this.payload = Optional.ofNullable(payload).orElse(Map.of());
    }

    public Port(SearchResponse.FeedDetails feedDetails) {
        this(feedDetails.getFeedId().getId(),
                feedDetails.getPropertiesList().stream().map(MyProperty::factory).toList(), feedDetails.getStoreLast(), Map.of());
    }

    public Port(SearchResponse.InputDetails inputDetails) {
        this(inputDetails.getInputId().getId(),
                inputDetails.getPropertiesList().stream().map(MyProperty::factory).toList(), false, Map.of());
    }

    public String id() {
        return id;
    }

    public List<MyProperty> properties() {
        return properties;
    }

    public boolean storeLast() {
        return storeLast;
    }

    public Map<String, Object> payload() {
        return payload;
    }

    public synchronized void updatePayload(Map<String, Object> newPayload) {
        if (payload == null) {
            payload = new ConcurrentHashMap<>();
        } else {
            payload.clear();
            payload.putAll(newPayload);
        }
    }

    public Optional<String> payloadAsJson() {
        if (payload == null) {
            return Optional.empty();
        }
        Gson gson = new Gson();
        return Optional.of(gson.toJson(payload()));
    }

    @Override
    public String toString() {
        return "Port{" +
                "id='" + id + '\'' +
                '}';
    }
}
