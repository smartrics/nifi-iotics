package smartrics.iotics.nifi.processors.objects;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import com.iotics.api.*;
import smartrics.iotics.connectors.twins.AbstractTwin;
import smartrics.iotics.connectors.twins.MappableMaker;
import smartrics.iotics.connectors.twins.Mapper;
import smartrics.iotics.host.Builders;
import smartrics.iotics.host.IoticsApi;
import smartrics.iotics.identity.Identity;
import smartrics.iotics.identity.SimpleIdentityManager;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import static smartrics.iotics.nifi.processors.Constants.RDF;
import static smartrics.iotics.nifi.processors.Constants.RDFS;

public class FollowerTwin extends AbstractTwin implements MappableMaker, Mapper {
    private final FollowerModel followerModel;

    public FollowerTwin(FollowerModel model, IoticsApi api, SimpleIdentityManager sim, Identity myIdentity) {
        super(api, sim, myIdentity);
        this.followerModel = model;
    }

    public record FollowerModel(String label, String comment, String type) {
    }

    @Override
    public Mapper getMapper() {
        return this;
    }

    @Override
    public UpsertTwinRequest getUpsertTwinRequest() {
        UpsertTwinRequest.Builder reqBuilder = UpsertTwinRequest.newBuilder()
                .setHeaders(Builders.newHeadersBuilder(super.getAgentIdentity()));
        Identity id = super.getMyIdentity();
        UpsertTwinRequest.Payload.Builder payloadBuilder = UpsertTwinRequest.Payload.newBuilder();
        payloadBuilder.setTwinId(TwinID.newBuilder().setId(id.did()));
        payloadBuilder.addProperties(Property.newBuilder().setKey(RDFS + "label")
                .setLiteralValue(Literal.newBuilder().setValue(followerModel.label()).build()).build());
        payloadBuilder.addProperties(Property.newBuilder().setKey(RDFS + "comment")
                .setLiteralValue(Literal.newBuilder().setValue(followerModel.comment()).build()).build());

        payloadBuilder.addProperties(Property.newBuilder().setKey(RDF + "type").setUriValue(Uri.newBuilder()
                .setValue(followerModel.type()).build()).build());
        payloadBuilder.addProperties(Property.newBuilder().setKey("http://data.iotics.com/public#hostAllowList")
                .setUriValue(Uri.newBuilder().setValue("http://data.iotics.com/public#allHosts").build()).build());

        UpsertFeedWithMeta.Builder builder = UpsertFeedWithMeta.newBuilder().setId("status")
                .setStoreLast(true)
                .addProperties(Property.newBuilder().setKey(RDFS + "comment").setLiteralValue(Literal.newBuilder().setValue("Current operational status of this follower").build()).build())
                .addProperties(Property.newBuilder().setKey(RDFS + "label").setLiteralValue(Literal.newBuilder().setValue("OperationalStatus").build()).build());
        builder.addValues(Value.newBuilder().setLabel("isOperational").setDataType("boolean").setComment("true if operational").build());

        payloadBuilder.addFeeds(builder.build());

        UpsertTwinRequest.Payload payload = payloadBuilder.build();
        reqBuilder.setPayload(payload);
        return reqBuilder.build();
    }

    @Override
    public List<ShareFeedDataRequest> getShareFeedDataRequest() {
        FeedID statusFeedID = FeedID.newBuilder()
                .setTwinId(super.getMyIdentity().did())
                .setId("status")
                .build();

        ShareFeedDataRequest request = ShareFeedDataRequest.newBuilder()
                .setHeaders(Builders.newHeadersBuilder(super.getAgentIdentity()).build())
                .setPayload(ShareFeedDataRequest.Payload.newBuilder().setSample(FeedData.newBuilder()
                        .setData(ByteString.copyFrom(makeStatusPayload().getBytes(StandardCharsets.UTF_8))).build()).build())
                .setArgs(ShareFeedDataRequest.Arguments.newBuilder()
                        .setFeedId(statusFeedID).build()).build();

        return Lists.newArrayList(request);
    }

    public String makeStatusPayload() {
        Map<String, String> values = new HashMap<>();
        values.put("value", "OK");
        return new Gson().toJson(values);
    }

}
