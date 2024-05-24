package smartrics.iotics.nifi.processors.objects;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.iotics.api.DescribeTwinResponse;
import com.iotics.api.SearchResponse;
import com.iotics.api.TwinID;

import java.util.List;
import java.util.Optional;

public record MyTwinModel(String hostId, String id, List<MyProperty> properties, List<Port> feeds, List<Port> inputs) {

    private static final Gson gson = new GsonBuilder()
            .registerTypeAdapter(MyTwinModel.class, new MyTwinModelDeserializer())
            .registerTypeAdapter(Port.class, new PortDeserializer())
            .create();

    public MyTwinModel(String hostId, String id) {
        this(hostId, id, List.of(), List.of(), List.of());
    }

    public MyTwinModel(DescribeTwinResponse.Payload payload) {
        this(payload.getTwinId().getHostId(), payload.getTwinId().getId(),
                payload.getResult().getPropertiesList().stream().map(MyProperty::factory).toList(),
                payload.getResult().getFeedsList().stream().map(Port::factory).toList(),
                payload.getResult().getInputsList().stream().map(Port::factory).toList());
    }

    public MyTwinModel(TwinID twinID) {
        this(twinID.getHostId(), twinID.getId());
    }

    public MyTwinModel(SearchResponse.TwinDetails twinDetails) {
        this(twinDetails.getTwinId().getHostId(),
                twinDetails.getTwinId().getId(),
                twinDetails.getPropertiesList().stream().map(MyProperty::factory).toList(),
                twinDetails.getFeedsList().stream().map(Port::new).toList(),
                twinDetails.getInputsList().stream().map(Port::new).toList());
    }

    public static MyTwinModel fromJson(String json) {
        return gson.fromJson(json, MyTwinModel.class);
    }

    public Optional<MyProperty> findProperty(String key) {
        return properties.stream().filter(p -> p.key().equals(key)).findFirst();
    }

    public String toJson() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

    public static final class Builder {
        private String hostId;
        private String id;
        private List<MyProperty> properties;
        private List<Port> feeds;
        private List<Port> inputs;

        private Builder() {
        }

        public static Builder aMyTwinModel() {
            return new Builder();
        }

        public Builder withHostId(String hostId) {
            this.hostId = hostId;
            return this;
        }

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public Builder withProperties(List<MyProperty> properties) {
            this.properties = properties;
            return this;
        }

        public Builder withFeeds(List<Port> feeds) {
            this.feeds = feeds;
            return this;
        }

        public Builder withInputs(List<Port> inputs) {
            this.inputs = inputs;
            return this;
        }

        public MyTwinModel build() {
            return new MyTwinModel(hostId, id,
                    Optional.ofNullable(properties).orElse(List.of()),
                    Optional.ofNullable(feeds).orElse(List.of()),
                    Optional.ofNullable(inputs).orElse(List.of())
            );
        }
    }
}
