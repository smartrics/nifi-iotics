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

import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.ByteString;
import com.iotics.api.FeedData;
import com.iotics.api.FeedID;
import com.iotics.api.ShareFeedDataRequest;
import com.iotics.api.ShareFeedDataResponse;
import io.grpc.stub.StreamObserver;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.jetbrains.annotations.NotNull;
import smartrics.iotics.host.Builders;
import smartrics.iotics.host.IoticsApi;
import smartrics.iotics.identity.SimpleIdentityManager;
import smartrics.iotics.nifi.processors.objects.MyProperty;
import smartrics.iotics.nifi.processors.objects.MyTwinModel;
import smartrics.iotics.nifi.processors.objects.Port;
import smartrics.iotics.nifi.services.IoticsHostService;

import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static smartrics.iotics.nifi.processors.Constants.*;

@Tags({"IOTICS", "DIGITAL TWIN", "PUBLISH"})
@CapabilityDescription("""
Processor for IOTICS to publish data over one or more feeds.
""")
public class IoticsPublisher extends AbstractProcessor {

    private static final Gson gson = new Gson();
    private final EventBus eventBus = new EventBus();
    private final Map<String, StreamObserver<ShareFeedDataRequest>> cache = new ConcurrentHashMap<>();
    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;
    private IoticsApi ioticsApi;
    private SimpleIdentityManager sim;
    private ExecutorService executor;

    private static void transferFailure(StreamEvent event, Throwable t) {
        String json = gson.toJson(new PublishFailure(event.myTwin(), t.getMessage()), new TypeToken<PublishFailure>() {
        }.getType());
        transfer(event, json, FAILURE);
    }

    private static void transferSuccess(StreamEvent event) {
        String json = gson.toJson(event.myTwin(), new TypeToken<MyTwinModel>() {
        }.getType());
        transfer(event, json, SUCCESS);
    }

    private static void transfer(StreamEvent event, String json, Relationship rel) {
        ProcessSession session = event.session();
        FlowFile ff = session.create(event.flowFile());
        session.write(ff, out -> {
            out.write(json.getBytes(StandardCharsets.UTF_8));
        });
        session.transfer(ff, rel);
        event.latchFeeds().countDown();
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(ID_PROP);
        descriptors.add(IOTICS_HOST_SERVICE);
        descriptors = Collections.unmodifiableList(descriptors);

        relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(ORIGINAL);
        relationships.add(FAILURE);
        relationships = Collections.unmodifiableSet(relationships);

        StreamNewFeedListener listener = new StreamNewFeedListener();
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
        this.sim = ioticsHostService.getSimpleIdentityManager();
        this.executor = ioticsHostService.getExecutor();

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            getLogger().warn("no flowfile found - not publishing");
            return;
        }

        AtomicReference<CountDownLatch> latchFeedsRef = new AtomicReference<>();
        session.read(flowFile, in -> {
            try {
                JsonElement jsonElement = JsonParser.parseReader(new InputStreamReader(in));
                Gson gson = new Gson();
                List<MyTwinModel> receivedTwins;
                if (jsonElement.isJsonArray()) {
                    // Specify the list type using TypeToken
                    Type listType = new TypeToken<List<MyTwinModel>>() {
                    }.getType();

                    // Convert the JsonElement to a List<MyCustomClass>
                    receivedTwins = gson.fromJson(jsonElement, listType);
                } else {
                    Type type = new TypeToken<MyTwinModel>() {
                    }.getType();
                    MyTwinModel myTwin = gson.fromJson(jsonElement, type);
                    receivedTwins = Lists.newArrayList(myTwin);
                }

                // latch to # of streams, then pass in the output stream and dec in the
                // onNext or onError to make sure we unblock when all sharing occurred

                int feedsCount = receivedTwins.stream().mapToInt(myTwinModel -> myTwinModel.feeds().size()).sum();
                latchFeedsRef.set(new CountDownLatch(feedsCount));

                receivedTwins.forEach(myTwin -> {
                    myTwin.feeds().forEach(port -> eventBus.post(new StreamEvent(session, flowFile, latchFeedsRef.get(), myTwin, port)));
                });
            } catch (Throwable t) {
                throw new ProcessException("error handling flowfile", t);
            }
        });
        try {
            latchFeedsRef.get().await();
            session.transfer(flowFile, ORIGINAL);
        } catch (InterruptedException e) {
            session.transfer(flowFile, FAILURE);
            throw new RuntimeException(e);
        }
    }

    private void shareFeed(StreamEvent event) {
        try {
            Optional<ShareFeedDataRequest> request = newShareFeedDataRequest(event);
            if (request.isEmpty()) {
                return;
            }
            ListenableFuture<ShareFeedDataResponse> res = ioticsApi.feedAPIFuture().shareFeedData(request.get());
            Futures.addCallback(res, new FutureCallback<>() {

                @Override
                public void onSuccess(ShareFeedDataResponse result) {
                    try {
                        transferSuccess(event);
                    } catch (Exception e) {
                        transferFailure(event, e);
                    }
                }

                @Override
                public void onFailure(@NotNull Throwable t) {
                    transferFailure(event, t);
                }
            }, this.executor);
        } catch (Exception e) {
            transferFailure(event, e);
        }
    }

    private Optional<ShareFeedDataRequest> newShareFeedDataRequest(StreamEvent event) {
        if (event.port().valuesAsJson().keySet().isEmpty()) {
            return Optional.empty();
        }
        Gson g = new Gson();
        String jsonString = g.toJson(event.port().valuesAsJson());
        return Optional.of(ShareFeedDataRequest.newBuilder()
                .setHeaders(Builders.newHeadersBuilder(sim.agentIdentity()))
                .setArgs(ShareFeedDataRequest.Arguments.newBuilder()
                        .setFeedId(FeedID.newBuilder()
                                .setTwinId(event.myTwin().id())
                                .setId(event.port().id())
                                .build())
                        .build())
                .setPayload(ShareFeedDataRequest.Payload.newBuilder()
                        .setSample(FeedData.newBuilder()
                                .setData(ByteString.copyFromUtf8(jsonString)))
                        .build())
                .build());
    }

    private String makeCacheKey(StreamEvent event) {
        return event.myTwin().hostDid() + "/" + event.myTwin().id() + "/" + event.port().id();
    }

    public record StreamEvent(ProcessSession session, FlowFile flowFile, CountDownLatch latchFeeds, MyTwinModel myTwin,
                              Port port) {
    }

    public record PublishFailure(MyTwinModel twin, String error) {

    }

    public class StreamNewFeedListener {

        @Subscribe
        public void shareEvent(IoticsPublisher.StreamEvent event) {
            shareFeed(event);
        }
    }
}
