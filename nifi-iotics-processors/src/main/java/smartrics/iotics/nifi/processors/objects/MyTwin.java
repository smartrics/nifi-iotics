package smartrics.iotics.nifi.processors.objects;

import com.iotics.api.SearchResponse;

import java.util.List;

public record MyTwin(String hostDid, String id, String keyName, List<MyProperty> properties, List<Port> feeds, List<Port> inputs) {
    public MyTwin(SearchResponse.TwinDetails twinDetails) {
        this(twinDetails.getTwinId().getHostId(),
                twinDetails.getTwinId().getId(),
                null,
                twinDetails.getPropertiesList().stream().map(MyProperty::Factory).toList(),
                twinDetails.getFeedsList().stream().map(Port::new).toList(),
                twinDetails.getInputsList().stream().map(Port::new).toList());
    }
}
