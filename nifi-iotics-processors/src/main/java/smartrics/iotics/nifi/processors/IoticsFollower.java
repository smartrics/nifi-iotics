/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package smartrics.iotics.nifi.processors;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.gson.Gson;
import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.iotics.api.*;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.jetbrains.annotations.NotNull;
import smartrics.iotics.host.Builders;
import smartrics.iotics.host.IoticsApi;
import smartrics.iotics.identity.Identity;
import smartrics.iotics.identity.SimpleIdentityManager;
import smartrics.iotics.nifi.processors.objects.FollowerTwin;
import smartrics.iotics.nifi.processors.objects.MyTwin;
import smartrics.iotics.nifi.processors.objects.Port;
import smartrics.iotics.nifi.services.IoticsHostService;

import java.io.InputStreamReader;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.apache.nifi.processor.util.StandardValidators.NON_EMPTY_VALIDATOR;
import static smartrics.iotics.nifi.processors.Constants.*;

@Tags({"IOTICS", "DIGITAL TWIN", "FIND", "BIND", "FOLLOWER"})
@CapabilityDescription("""
        Find and Bind to feeds. The processor runs an IOTICS search and, of the twins it finds, it follows all the feeds.
        Future enhancements: batch and batch sizes to improve performances
        """)
@WritesAttributes({
        @WritesAttribute(attribute = "followerTwinDid", description = "this follower's did"),
        @WritesAttribute(attribute = "hostDid", description = "the host where the share came from"),
        @WritesAttribute(attribute = "twinDid", description = "the twin where the share came from"),
        @WritesAttribute(attribute = "feedId", description = "the feed ID"),
        @WritesAttribute(attribute = "mimeType", description = "the content of the feed share"),
        @WritesAttribute(attribute = "occurredAt", description = "when the share occurredAt"),
})
public class IoticsFollower extends AbstractProcessor {
    public static PropertyDescriptor FOLLOWER_LABEL = new PropertyDescriptor
            .Builder().name("followerTwinLabel")
            .displayName("Follower Twin Label")
            .description("The label of the follower twin binding to the found feeds")
            .defaultValue("NiFi Follower Twin <some Unique id>")
            .required(true)
            .addValidator(NON_EMPTY_VALIDATOR)
            .build();
    public static PropertyDescriptor FOLLOWER_ID = new PropertyDescriptor
            .Builder().name("followerTwinUniqueId")
            .displayName("Follower Twin Unique ID")
            .description("An ID for this follower to differentiate it from other followers within this connector")
            .required(true)
            .addValidator(NON_EMPTY_VALIDATOR)
            .build();
    public static PropertyDescriptor FOLLOWER_COMMENT = new PropertyDescriptor
            .Builder().name("followerTwinComment")
            .displayName("Follower Twin Comment")
            .description("The comment of the follower twin binding to the found feeds")
            .defaultValue("An instance of a NiFi Follower Twin")
            .required(true)
            .addValidator(NON_EMPTY_VALIDATOR)
            .build();
    public static PropertyDescriptor FOLLOWER_CLASSIFIER = new PropertyDescriptor
            .Builder().name("followerTwinClassifier")
            .displayName("Follower Twin Classifier")
            .description("The RDF type of the follower twin binding to the found feeds")
            .defaultValue("https://schema.org/SoftwareApplication")
            .required(true)
            .addValidator(NON_EMPTY_VALIDATOR)
            .build();

    private final EventBus eventBus = new EventBus();
    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;
    private IoticsApi ioticsApi;
    private SimpleIdentityManager sim;
    private ExecutorService executor;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(FOLLOWER_ID);
        descriptors.add(FOLLOWER_LABEL);
        descriptors.add(FOLLOWER_COMMENT);
        descriptors.add(FOLLOWER_CLASSIFIER);

        descriptors.add(IOTICS_HOST_SERVICE);
        descriptors = Collections.unmodifiableList(descriptors);

        relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(ORIGINAL);
        relationships.add(FAILURE);
        relationships = Collections.unmodifiableSet(relationships);

        FollowEventListener listener = new FollowEventListener();
        eventBus.register(listener);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        IoticsHostService ioticsHostService =
                context.getProperty(IOTICS_HOST_SERVICE).asControllerService(IoticsHostService.class);

        this.ioticsApi = ioticsHostService.getIoticsApi();
        this.sim = ioticsHostService.getSimpleIdentityManager();
        this.executor = ioticsHostService.getExecutor();

        AtomicReference<MyTwin> myTwinRef = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        session.read(flowFile, in -> {
            Gson gson = new Gson();
            try {
                MyTwin myTwin = gson.fromJson(new InputStreamReader(in), MyTwin.class);
                myTwinRef.set(myTwin);
            } catch (Exception e) {
                getLogger().error("Failed to read data from FlowFile content", e);
            }
            latch.countDown();
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            session.transfer(flowFile, FAILURE);
            getLogger().error("interrupted while waiting for reading flow file");
            return;
        }
        if (myTwinRef.get() == null) {
            getLogger().error("follow twin not available from flow file");
            session.transfer(flowFile, FAILURE);
            return;
        }
        getLogger().info("follow twin available from flow file: " + myTwinRef.get());

        Consumer<String> consumer = followerDid ->
                eventBus.post(new FollowEvent(followerDid, myTwinRef.get(), context, session));
        makeFollowerTwin(context, consumer);
    }

    private void makeFollowerTwin(ProcessContext context, Consumer<String> onSuccess) {
        String label = context.getProperty(FOLLOWER_LABEL).getValue();
        String comment = context.getProperty(FOLLOWER_COMMENT).getValue();
        String type = context.getProperty(FOLLOWER_CLASSIFIER).getValue();
        String uniqueKeyName = context.getProperty(FOLLOWER_ID).getValue();

        FollowerTwin.FollowerModel model = new FollowerTwin.FollowerModel(label, comment, type);
        Identity ide = this.sim.newTwinIdentityWithControlDelegation(uniqueKeyName, "#deleg-" + uniqueKeyName.hashCode());
        FollowerTwin twin = new FollowerTwin(model, ioticsApi, sim, ide);
        Futures.addCallback(twin.upsert(), new FutureCallback<>() {
            @Override
            public void onSuccess(UpsertTwinResponse result) {
                String followerDid = result.getPayload().getTwinId().getId();
                getLogger().info("Follower twin created with did=" + followerDid);
                onSuccess.accept(followerDid);
            }

            @Override
            public void onFailure(@NotNull Throwable t) {
                getLogger().warn("Failed to make follower twin", t);
            }
        }, executor);
    }

    @OnStopped
    public void onStopped() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        ioticsApi.stop(Duration.ofSeconds(5));
    }


    private void follow(FollowEvent event) {
        event.twin().feeds().forEach(port -> eventBus.post(new FollowFeedEvent(event, port)));
    }

    private Consumer<FetchInterestResponse> feedDataConsumer(String followerDid, ProcessContext context, ProcessSession session) {
        return fetchInterestResponse -> {
            FetchInterestResponse.Payload payload = fetchInterestResponse.getPayload();
            FeedID followedFeedId = payload.getInterest().getFollowedFeedId();
            FeedData feedData = payload.getFeedData();
            ByteString data = feedData.getData();
            FlowFile ff = session.create();
            try {
                session.write(ff, out -> out.write(data.toByteArray()));
                session.putAttribute(ff, "followerTwinDid", followerDid);
                session.putAttribute(ff, "hostDid", followedFeedId.getHostId());
                session.putAttribute(ff, "twinDid", followedFeedId.getTwinId());
                session.putAttribute(ff, "feedId", followedFeedId.getId());
                session.putAttribute(ff, "mimeType", feedData.getMime());
                session.putAttribute(ff, "occurredAt", feedData.getOccurredAt().toString());
                session.transfer(ff, SUCCESS);
                session.commitAsync(() -> getLogger().info("successfully transferred FlowFile"),
                        throwable -> getLogger().error("failed to transfer FlowFile", throwable));
            } catch (Exception e) {
                getLogger().error("exception when creating session", e);
                session.transfer(ff, FAILURE);
            }
            context.yield();
        };
    }

    private void follow(FollowFeedEvent ev) {

        String followerDid = ev.followEvent().followerDid();
        MyTwin twin = ev.followEvent().twin();
        String twinDid = twin.id();
        String feedId = ev.port().id();
        ProcessSession session = ev.followEvent().session();
        ProcessContext context = ev.followEvent().context();

        FetchInterestRequest request = newFetchInterestRequest(followerDid, twin, ev.port());

        getLogger().info("FOLLOW {}/{}", twinDid, feedId);
        Consumer<FetchInterestResponse> consumer = feedDataConsumer(followerDid, context, session);
        session.transfer(session.get(), ORIGINAL);
        this.ioticsApi.interestAPI().fetchInterests(request, new StreamObserver<>() {
            @Override
            public void onNext(FetchInterestResponse fetchInterestResponse) {
                consumer.accept(fetchInterestResponse);
            }

            @Override
            public void onError(Throwable throwable) {
                getLogger().error("FOLLOW ERROR {}/{}", twinDid, feedId, throwable);
                MyFlowFileFilter filter = new MyFlowFileFilter(ev);
                List<FlowFile> found = session.get(filter);
                if (throwable instanceof StatusRuntimeException
                        && ((StatusRuntimeException) throwable).getStatus().getCode() == Status.Code.UNAUTHENTICATED
                ) {
                    // TODO seems the ff isn't found in unittest - check live deployment
                    if (!found.isEmpty()) {
                        getLogger().info("RE-FOLLOW {}/{}", twinDid, feedId);
                        eventBus.post(ev.followEvent);
                    }
                } else {
                    found.forEach(flowFile -> session.transfer(flowFile, FAILURE));
                }
            }

            @Override
            public void onCompleted() {
                getLogger().info("FOLLOW COMPLETE {}/{}", twinDid, feedId);
            }
        });
    }

    @NotNull
    private FetchInterestRequest newFetchInterestRequest(String followerDid, MyTwin twin, Port port) {
        return FetchInterestRequest.newBuilder()
                .setHeaders(Builders.newHeadersBuilder(sim.agentIdentity()))
                .setFetchLastStored(BoolValue.newBuilder().setValue(true).build())
                .setArgs(FetchInterestRequest.Arguments.newBuilder()
                        .setInterest(Interest.newBuilder()
                                .setFollowedFeedId(FeedID.newBuilder()
                                        .setHostId(twin.hostDid())
                                        .setTwinId(twin.id())
                                        .setId(port.id()).build())
                                .setFollowerTwinId(TwinID.newBuilder()
                                        .setId(followerDid)
                                        .build())
                                .build())
                        .build())
                .build();
    }

    private record MyFlowFileFilter(FollowFeedEvent ev) implements FlowFileFilter {

        @Override
        public FlowFileFilterResult filter(FlowFile flowFile) {
            String hostDid = flowFile.getAttribute("hostDid");
            String twinDid = flowFile.getAttribute("twinDid");
            String feedId = flowFile.getAttribute("feedId");

            if (hostDid != null && hostDid.equals(ev.followEvent().twin().hostDid())) {
                if (twinDid != null && twinDid.equals(ev.followEvent().twin().id())) {
                    if (feedId != null && feedId.equals(ev.port().id())) {
                        return FlowFileFilter.FlowFileFilterResult.ACCEPT_AND_CONTINUE;
                    }
                }
            }
            return FlowFileFilter.FlowFileFilterResult.REJECT_AND_CONTINUE;
        }
    }

    public record FollowEvent(String followerDid, MyTwin twin, ProcessContext context, ProcessSession session) {
    }

    public record FollowFeedEvent(FollowEvent followEvent, Port port) {
    }

    public class FollowEventListener {

        @Subscribe
        public void onFollowEvent(FollowEvent event) {
            follow(event);
        }

        @Subscribe
        public void onFollowEvent(FollowFeedEvent event) {
            follow(event);
        }
    }
}
