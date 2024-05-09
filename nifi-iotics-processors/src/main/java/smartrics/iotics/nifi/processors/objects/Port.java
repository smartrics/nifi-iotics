package smartrics.iotics.nifi.processors.objects;

import com.google.gson.Gson;
import com.iotics.api.SearchResponse;

import java.util.List;
import java.util.Map;

public record Port(String id, List<MyProperty> properties, Map<String, Object> payload) {

    public Port(SearchResponse.FeedDetails feedDetails) {
        this(feedDetails.getFeedId().getId(),
                feedDetails.getPropertiesList().stream().map(MyProperty::Factory).toList(), null);
    }

    public Port(SearchResponse.InputDetails inputDetails) {
        this(inputDetails.getInputId().getId(),
                inputDetails.getPropertiesList().stream().map(MyProperty::Factory).toList(), null);
    }

    public String payloadAsJson() {
        if (payload == null) {
            return null;
        }
        Gson gson = new Gson();
        return gson.toJson(payload());
    }
}
