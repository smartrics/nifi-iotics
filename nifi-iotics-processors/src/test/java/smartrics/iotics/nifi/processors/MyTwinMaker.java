package smartrics.iotics.nifi.processors;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import com.iotics.api.*;
import smartrics.iotics.connectors.twins.*;
import smartrics.iotics.host.Builders;
import smartrics.iotics.host.IoticsApi;
import smartrics.iotics.identity.Identity;
import smartrics.iotics.identity.SimpleIdentityManager;
import smartrics.iotics.nifi.processors.objects.MyProperty;
import smartrics.iotics.nifi.processors.objects.MyTwin;
import smartrics.iotics.nifi.services.BasicIoticsHostService;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class MyTwinMaker extends AbstractTwin implements MappableMaker, MappablePublisher, Mapper {
    public static MyTwinMaker makeMyTwin(BasicIoticsHostService service) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        String content = Files.readString(Path.of("src\\test\\resources\\twin_with_feed.json"));
        Gson gson = new Gson();
        MyTwin myTwin = gson.fromJson(content, MyTwin.class);
        Identity twinIdentity = service.getSimpleIdentityManager().newTwinIdentityWithControlDelegation(myTwin.id(), "#master");
        MyTwinMaker myTwinMaker = new MyTwinMaker(myTwin, service.getIoticsApi(), service.getSimpleIdentityManager(), twinIdentity);
        ListenableFuture<UpsertTwinResponse> res = myTwinMaker.upsert();
        AtomicReference<TwinID> ref = new AtomicReference<>();
        res.addListener(() -> {
            try {
                TwinID twinID = res.resultNow().getPayload().getTwinId();
                ref.set(twinID);
            } catch (Exception e) {
                res.exceptionNow().printStackTrace();
            }
            latch.countDown();
        }, service.getExecutor());
        latch.await();
        TwinID twinID = ref.get();
        if(twinID == null) {
            throw new IllegalStateException("operation not completed - twinID is null");
        }
        MyTwin myTwinWithId = new MyTwin(twinID.getHostId(), twinID.getId(), myTwin.properties(), myTwin.feeds(), myTwin.inputs());
        return new MyTwinMaker(myTwinWithId, service.getIoticsApi(), service.getSimpleIdentityManager(), twinIdentity);
    }

    private final MyTwin twin;

    public MyTwinMaker(MyTwin twin, IoticsApi ioticsApi, SimpleIdentityManager sim, Identity myIdentity) {
        super(ioticsApi, sim, myIdentity);
        this.twin = twin;
    }

    public MyTwin twin() {
        return twin;
    }

    @Override
    public Mapper getMapper() {
        return this;
    }

    @Override
    public UpsertTwinRequest getUpsertTwinRequest() {
        UpsertTwinRequest.Payload.Builder pBuilder = UpsertTwinRequest.Payload.newBuilder();
        pBuilder.setTwinId(TwinID.newBuilder().setId(getMyIdentity().did()).build());
        this.twin.properties().forEach(myProperty -> pBuilder.addProperties(addPropertyValue(Property.newBuilder(), myProperty).setKey(myProperty.key()).build()));
        this.twin.feeds().forEach(port -> {
            UpsertFeedWithMeta.Builder fBuilder = UpsertFeedWithMeta.newBuilder();
            fBuilder.setStoreLast(port.storeLast()).setId(port.id());
            port.properties().forEach(myProperty -> fBuilder.addProperties(addPropertyValue(Property.newBuilder(), myProperty).setKey(myProperty.key()).build()));
            // values are made up here for test purposes
            fBuilder.addValues(Value.newBuilder().setLabel("data").setDataType("string").setComment("sample data").build());
            pBuilder.addFeeds(fBuilder.build());
        });
        // do inputs when needed
        return UpsertTwinRequest.newBuilder()
                .setHeaders(Builders.newHeadersBuilder(getAgentIdentity()))
                .setPayload(pBuilder.build())
                .build();
    }

    private Property.Builder addPropertyValue(Property.Builder builder, MyProperty myProperty) {
        switch (myProperty.type()) {
            case "StringLiteral" ->
                    builder.setStringLiteralValue(StringLiteral.newBuilder().setValue(myProperty.value()).build());
            case "Literal" ->
                    builder.setLiteralValue(Literal.newBuilder().setDataType(myProperty.dataType()).setValue(myProperty.value()).build());
            case "Uri" -> builder.setUriValue(Uri.newBuilder().setValue(myProperty.value()).build());
            case "LangLiteral" ->
                    builder.setLangLiteralValue(LangLiteral.newBuilder().setLang(myProperty.lang()).setValue(myProperty.value()).build());
            case null, default -> throw new IllegalArgumentException("unsupported property: " + myProperty);
        }
        return builder;
    }

    public void updatePayload() {
        // feeds have a single payload label called data
        this.twin.feeds().forEach(f -> {
            f.updatePayload(Map.of("data", UUID.randomUUID().toString()));
        });
    }

    @Override
    public List<ShareFeedDataRequest> getShareFeedDataRequest() {
        return this.twin.feeds().stream().filter(p -> p.payloadAsJson().isPresent()).map(port -> ShareFeedDataRequest.newBuilder()
                .setHeaders(Builders.newHeadersBuilder(getAgentIdentity()))
                .setArgs(ShareFeedDataRequest.Arguments.newBuilder()
                        .setFeedId(FeedID.newBuilder()
                                .setId(port.id())
                                .setTwinId(this.twin.id())
                                .build())
                        .build())
                .setPayload(ShareFeedDataRequest.Payload.newBuilder()
                        .setSample(FeedData.newBuilder()
                                .setData(ByteString.copyFrom(port.payloadAsJson().orElseThrow().getBytes(StandardCharsets.UTF_8)))
                                .build())
                        .build())
                .build()).toList();

    }
}
