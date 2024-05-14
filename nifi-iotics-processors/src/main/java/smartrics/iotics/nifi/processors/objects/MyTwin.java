package smartrics.iotics.nifi.processors.objects;

import com.google.gson.Gson;
import com.iotics.api.SearchResponse;

import java.util.List;

public record MyTwin(String hostDid, String id, List<MyProperty> properties, List<Port> feeds,
                     List<Port> inputs) {
    public MyTwin(SearchResponse.TwinDetails twinDetails) {
        this(twinDetails.getTwinId().getHostId(),
                twinDetails.getTwinId().getId(),
                twinDetails.getPropertiesList().stream().map(MyProperty::Factory).toList(),
                twinDetails.getFeedsList().stream().map(Port::new).toList(),
                twinDetails.getInputsList().stream().map(Port::new).toList());
    }

    public String toJson() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}
