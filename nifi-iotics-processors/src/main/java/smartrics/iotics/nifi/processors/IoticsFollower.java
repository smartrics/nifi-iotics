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
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.jetbrains.annotations.NotNull;
import smartrics.iotics.identity.Identity;
import smartrics.iotics.nifi.processors.objects.FollowerTwin;
import smartrics.iotics.nifi.processors.objects.MyTwin;
import smartrics.iotics.nifi.processors.objects.Port;
import smartrics.iotics.nifi.services.IoticsHostService;
import smartrics.iotics.space.Builders;
import smartrics.iotics.space.grpc.IoticsApi;

import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import static org.apache.nifi.processor.util.StandardValidators.*;
import static smartrics.iotics.nifi.processors.Constants.*;

@Tags({"IOTICS", "DIGITAL TWIN", "FIND", "BIND", "FOLLOWER"})
@CapabilityDescription("""
        Find and Bind to feeds. The processor runs an IOTICS search and, of the twins it finds, it follows all the feeds.
        Future enhancements: batch and batch sizes to improve performances
        """)
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
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        IoticsHostService ioticsHostService =
                context.getProperty(IOTICS_HOST_SERVICE).asControllerService(IoticsHostService.class);

        this.ioticsApi = ioticsHostService.getIoticsApi();
        this.executor = ioticsHostService.getExecutor();

        String label = context.getProperty(FOLLOWER_LABEL).getValue();
        String comment = context.getProperty(FOLLOWER_COMMENT).getValue();
        String type = context.getProperty(FOLLOWER_CLASSIFIER).getValue();
        String uniqueKeyName = context.getProperty(FOLLOWER_ID).getValue();

        FollowerTwin.FollowerModel model = new FollowerTwin.FollowerModel(label, comment, type);
        Identity ide = this.ioticsApi.getSim().newTwinIdentityWithControlDelegation(uniqueKeyName, "#deleg-" + uniqueKeyName.hashCode());
        FollowerTwin twin = new FollowerTwin(model, ioticsApi, ide, executor);

        CountDownLatch latch = new CountDownLatch(1);
        Futures.addCallback(twin.make(), new FutureCallback<>() {
            @Override
            public void onSuccess(UpsertTwinResponse result) {
                String followerDid = result.getPayload().getTwinId().getId();
                getLogger().info("Follower twin created with did=" + followerDid);
                eventBus.post(new FollowEvent(followerDid, session.get(), session));
            }

            @Override
            public void onFailure(@NotNull Throwable t) {
                getLogger().warn("Failed to make follower twin", t);
            }
        }, executor);

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new ProcessException(e);
        }

    }

    private void follow(FollowEvent event) {
        FlowFile flowFile = event.flowFile();
        if(flowFile == null) {
            getLogger().warn("no flowfile found - not following");
            return;
        }

        String followerDid = event.followerDid();
        ProcessSession session = event.session();

        session.read(flowFile, in -> {
            Gson gson = new Gson();
            try {
                MyTwin myTwin = gson.fromJson(new InputStreamReader(in), MyTwin.class);

                Consumer<FetchInterestResponse> consumer = fetchInterestResponse -> {
                    FlowFile ff = session.create(session.get());
                    try {
                        FeedID followedFeedId = fetchInterestResponse.getPayload().getInterest().getFollowedFeedId();
                        session.putAttribute(ff, "followerTwinDid", followerDid);
                        session.putAttribute(ff, "hostDid", followedFeedId.getHostId());
                        session.putAttribute(ff, "twinDid", followedFeedId.getTwinId());
                        session.putAttribute(ff, "feedId", followedFeedId.getId());
                        FeedData feedData = fetchInterestResponse.getPayload().getFeedData();
                        session.putAttribute(ff, "mimeType", feedData.getMime());
                        session.putAttribute(ff, "occurredAt", feedData.getOccurredAt().toString());
                        ByteString bytes = feedData.getData();
                        session.write(ff, out -> out.write(bytes.toByteArray()));
                        session.transfer(ff, SUCCESS);
                    } catch (Exception e) {
                        session.transfer(ff, FAILURE);
                    }
                };
                myTwin.feeds().forEach(port -> {
                    doFollow(followerDid, myTwin.hostDid(), myTwin.id(), session, port, consumer);
                });
            } catch (Exception ex) {
                getLogger().error("Failed to read json string.", ex);
                throw new ProcessException(ex.getMessage(), ex);
            }

        });
    }

    private void doFollow(String followerDid, String hostId, String twinId, ProcessSession session, Port feedDetails, Consumer<FetchInterestResponse> consumer) {
        FetchInterestRequest request = newFetchInterestRequest(followerDid, hostId, twinId, feedDetails);
        getLogger().info("FOLLOW {}/{}/{}", hostId, twinId, feedDetails.id());
        this.ioticsApi.interestAPIStub().fetchInterests(request, new StreamObserver<>() {
            @Override
            public void onNext(FetchInterestResponse fetchInterestResponse) {
                consumer.accept(fetchInterestResponse);
            }

            @Override
            public void onError(Throwable throwable) {
                // TODO: trigger re-follow on token expired
                getLogger().error("FOLLOW ERROR {}/{}/{}", hostId, twinId, feedDetails.id(), throwable);
                MyFlowFileFilter filter = new MyFlowFileFilter(hostId, twinId, feedDetails.id());
                List<FlowFile> found = session.get(filter);
                if (throwable instanceof StatusRuntimeException
                        && ((StatusRuntimeException) throwable).getStatus().getCode() == Status.Code.UNAUTHENTICATED
                ) {
                    // TODO seems the ff isn't found in unittest - check live deployment
                    if(!found.isEmpty()) {
                        getLogger().info("Follower twin following again did=" + followerDid);
                        eventBus.post(new FollowEvent(followerDid, found.getFirst(), session));
                    }
                } else {
                    found.forEach(flowFile -> session.transfer(flowFile, FAILURE));
                }
            }

            @Override
            public void onCompleted() {
                getLogger().info("FOLLOW COMPLETE {}/{}/{}", hostId, twinId, feedDetails.id());
                MyFlowFileFilter filter = new MyFlowFileFilter(hostId, twinId, feedDetails.id());
                // TODO seems the ff isn't found in unittest - check live deployment
                List<FlowFile> found = session.get(filter);
                found.forEach(flowFile -> session.transfer(flowFile, SUCCESS));
            }
        });
    }

    @NotNull
    private FetchInterestRequest newFetchInterestRequest(String followerDid, String hostId, String twinId, Port feedDetails) {
        return FetchInterestRequest.newBuilder()
                .setHeaders(Builders.newHeadersBuilder(ioticsApi.getSim().agentIdentity().did()))
                .setFetchLastStored(BoolValue.newBuilder().setValue(true).build())
                .setArgs(FetchInterestRequest.Arguments.newBuilder()
                        .setInterest(Interest.newBuilder()
                                .setFollowedFeedId(FeedID.newBuilder()
                                        .setHostId(hostId)
                                        .setTwinId(twinId)
                                        .setId(feedDetails.id()).build())
                                .setFollowerTwinId(TwinID.newBuilder()
                                        .setId(followerDid)
                                        .build())
                                .build())
                        .build())
                .build();
    }

    private record MyFlowFileFilter(String myHostDid, String myTwinDid, String myFeedId) implements FlowFileFilter {

        @Override
        public FlowFileFilterResult filter(FlowFile flowFile) {
            String hostDid = flowFile.getAttribute("hostDid");
            String twinDid = flowFile.getAttribute("twinDid");
            String feedId = flowFile.getAttribute("feedId");

            if (hostDid != null && hostDid.equals(myHostDid)) {
                if (twinDid != null && twinDid.equals(myTwinDid)) {
                    if (feedId != null && feedId.equals(myFeedId)) {
                        return FlowFileFilter.FlowFileFilterResult.ACCEPT_AND_CONTINUE;
                    }
                }
            }
            return FlowFileFilter.FlowFileFilterResult.REJECT_AND_CONTINUE;
        }
    }

    public record FollowEvent(String followerDid, FlowFile flowFile, ProcessSession session) {
    }

    public class FollowEventListener {

        @Subscribe
        public void onFollowEvent(FollowEvent event) {
            follow(event);
        }
    }
}
