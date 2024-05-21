package smartrics.iotics.nifi.processors.objects;

import com.google.gson.Gson;
import com.iotics.api.SearchResponse;

import java.util.List;
import java.util.Optional;

public record MyTwinModel(String hostDid, String id, List<MyProperty> properties, List<Port> feeds,
                          List<Port> inputs) {
    public MyTwinModel(SearchResponse.TwinDetails twinDetails) {
        this(twinDetails.getTwinId().getHostId(),
                twinDetails.getTwinId().getId(),
                twinDetails.getPropertiesList().stream().map(MyProperty::factory).toList(),
                twinDetails.getFeedsList().stream().map(Port::new).toList(),
                twinDetails.getInputsList().stream().map(Port::new).toList());
    }

    public Optional<MyProperty> findProperty(String key) {
        return properties.stream().filter(p -> p.key().equals(key)).findFirst();
    }

    public String toJson() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}
