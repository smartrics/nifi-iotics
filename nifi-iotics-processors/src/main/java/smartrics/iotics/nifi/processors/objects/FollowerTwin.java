package smartrics.iotics.nifi.processors.objects;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import com.iotics.api.*;
import smartrics.iotics.identity.Identity;
import smartrics.iotics.space.Builders;
import smartrics.iotics.space.grpc.IoticsApi;
import smartrics.iotics.space.twins.AbstractTwin;
import smartrics.iotics.space.twins.MappableMaker;
import smartrics.iotics.space.twins.Mapper;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import static smartrics.iotics.nifi.processors.Constants.RDF;
import static smartrics.iotics.nifi.processors.Constants.RDFS;

public class FollowerTwin extends AbstractTwin implements MappableMaker<FollowerTwin.FollowerModel>, Mapper<FollowerTwin.FollowerModel> {
    private final FollowerModel followerModel;

    public FollowerTwin(FollowerModel model, IoticsApi api, String keyName, Executor executor) {
        super(api, keyName, executor);
        this.followerModel = model;
    }

    public FollowerTwin(FollowerModel model, IoticsApi api, Identity identity, Executor executor) {
        super(api, identity, executor);
        this.followerModel = model;
    }

    public record FollowerModel(String label, String comment, String type) {
    }

    @Override
    public Mapper<FollowerModel> getMapper() {
        return this;
    }

    @Override
    public FollowerTwin.FollowerModel getTwinSource() {
        return this.followerModel;
    }

    @Override
    public Identity getTwinIdentity(FollowerTwin.FollowerModel followerModel) {
        return this.getIdentity();
    }

    @Override
    public UpsertTwinRequest getUpsertTwinRequest(FollowerTwin.FollowerModel followerModel) {
        UpsertTwinRequest.Builder reqBuilder = UpsertTwinRequest.newBuilder()
                .setHeaders(Builders.newHeadersBuilder(super.ioticsApi().getSim().agentIdentity().did()));
        Identity id = this.getIdentity();
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
                .addProperties(Property.newBuilder().setKey(RDFS + "comment").setLiteralValue(Literal.newBuilder().setValue("Current operational status of this station").build()).build())
                .addProperties(Property.newBuilder().setKey(RDFS + "label").setLiteralValue(Literal.newBuilder().setValue("OperationalStatus").build()).build());
        builder.addValues(Value.newBuilder().setLabel("isOperational").setDataType("boolean").setComment("true if operational").build());
        builder.addValues(Value.newBuilder().setLabel("isRecentlyVerified").setDataType("boolean").setComment("true if recently verified").build());

        payloadBuilder.addFeeds(builder.build());

        UpsertTwinRequest.Payload payload = payloadBuilder.build();
        reqBuilder.setPayload(payload);
        return reqBuilder.build();
    }

    @Override
    public List<ShareFeedDataRequest> getShareFeedDataRequest(FollowerTwin.FollowerModel followerModel) {
        FeedID statusFeedID = FeedID.newBuilder()
                .setTwinId(getIdentity().did())
                .setId("status")
                .build();

        ShareFeedDataRequest request = ShareFeedDataRequest.newBuilder()
                .setHeaders(Builders.newHeadersBuilder(ioticsApi().getSim().agentIdentity().did()).build())
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
