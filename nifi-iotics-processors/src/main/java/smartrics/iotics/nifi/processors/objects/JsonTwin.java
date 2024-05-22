package smartrics.iotics.nifi.processors.objects;

import com.google.protobuf.ByteString;
import com.iotics.api.*;
import smartrics.iotics.connectors.twins.AbstractTwin;
import smartrics.iotics.connectors.twins.MappableMaker;
import smartrics.iotics.connectors.twins.MappablePublisher;
import smartrics.iotics.connectors.twins.Mapper;
import smartrics.iotics.host.Builders;
import smartrics.iotics.host.IoticsApi;
import smartrics.iotics.identity.Identity;
import smartrics.iotics.identity.IdentityManager;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Function;

import static smartrics.iotics.nifi.processors.objects.MyProperty.factory;

public class JsonTwin extends AbstractTwin implements MappableMaker, MappablePublisher, Mapper {
    private final MyTwinModel model;

    public JsonTwin(IoticsApi api, IdentityManager sim, Identity myIdentity, MyTwinModel model) {
        super(api, sim, myIdentity);
        this.model = model;
    }

    public MyTwinModel getModel() {
        return model;
    }

    @Override
    public Mapper getMapper() {
        return this;
    }

    @Override
    public UpsertTwinRequest getUpsertTwinRequest() {
        UpsertTwinRequest.Builder reqBuilder = UpsertTwinRequest
                .newBuilder()
                .setHeaders(Builders.newHeadersBuilder(super.getAgentIdentity()));
        UpsertTwinRequest.Payload.Builder payloadBuilder = UpsertTwinRequest.Payload.newBuilder();
        payloadBuilder.setTwinId(TwinID.newBuilder().setId(super.getMyIdentity().did()));
        List<MyProperty> propertiesList = model.properties();
        propertiesList.forEach(prop -> {
            Property property = factory(prop);
            payloadBuilder.addProperties(property);
        });

        model.feeds().forEach(port -> payloadBuilder.addFeeds(Port.feedFactory(port)));
        model.inputs().forEach(port -> payloadBuilder.addInputs(Port.inputsFactory(port)));
        UpsertTwinRequest.Payload payload = payloadBuilder.build();
        reqBuilder.setPayload(payload);
        return reqBuilder.build();
    }

    @Override
    public List<ShareFeedDataRequest> getShareFeedDataRequest() {
        Function<Port, byte[]> getPayload = port -> port.valuesAsJson().getAsString().getBytes(StandardCharsets.UTF_8);
        return this.model.feeds().stream().map(port -> ShareFeedDataRequest.newBuilder()
                .setArgs(ShareFeedDataRequest.Arguments.newBuilder()
                        .setFeedId(FeedID.newBuilder()
                                .setId(port.id())
                                .setTwinId(JsonTwin.super.getMyIdentity().did())
                                .build())
                        .build())
                .setPayload(ShareFeedDataRequest.Payload.newBuilder()
                        .setSample(FeedData.newBuilder()
                                .setData(ByteString.copyFrom(getPayload.apply(port)))
                                .build())
                        .build())
                .setHeaders(Builders.newHeadersBuilder(getAgentIdentity())).build()).toList();
    }
}
