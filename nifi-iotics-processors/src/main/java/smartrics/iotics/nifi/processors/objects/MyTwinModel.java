package smartrics.iotics.nifi.processors.objects;

import com.google.gson.*;
import com.iotics.api.SearchResponse;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public record MyTwinModel(String hostDid, String id, List<MyProperty> properties, List<Port> feeds, List<Port> inputs) {

    private static final Gson gson = new GsonBuilder()
            .registerTypeAdapter(MyTwinModel.class, new MyTwinModelDeserializer())
            .registerTypeAdapter(Port.class, new PortDeserializer())
            .create();

    public static MyTwinModel fromJson(String json) {
        return gson.fromJson(json, MyTwinModel.class);
    }

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

    public static final class Builder {
        private String hostDid;
        private String id;
        private List<MyProperty> properties;
        private List<Port> feeds;
        private List<Port> inputs;

        private Builder() {
        }

        public static Builder aMyTwinModel() {
            return new Builder();
        }

        public Builder withHostDid(String hostDid) {
            this.hostDid = hostDid;
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
            return new MyTwinModel(hostDid, id,
                    Optional.ofNullable(properties).orElse(List.of()),
                    Optional.ofNullable(feeds).orElse(List.of()),
                    Optional.ofNullable(inputs).orElse(List.of())
            );
        }
    }
}
