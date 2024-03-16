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
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.ByteString;
import com.iotics.api.FeedData;
import com.iotics.api.FeedID;
import com.iotics.api.ShareFeedDataRequest;
import com.iotics.api.ShareFeedDataResponse;
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
import org.checkerframework.checker.units.qual.C;
import org.jetbrains.annotations.NotNull;
import smartrics.iotics.nifi.processors.objects.MyTwin;
import smartrics.iotics.nifi.processors.objects.Port;
import smartrics.iotics.nifi.services.IoticsHostService;
import smartrics.iotics.space.Builders;
import smartrics.iotics.space.grpc.IoticsApi;

import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

import static smartrics.iotics.nifi.processors.Constants.*;

@Tags({"IOTICS", "DIGITAL TWIN", "PUBLISH"})
@CapabilityDescription("""
        Processor for IOTICS publish data over a feed
        """)
public class IoticsPublisher extends AbstractProcessor {

    public static PropertyDescriptor DEBUG_FLAG = new PropertyDescriptor
            .Builder().name("debugFlag")
            .displayName("Debug flag")
            .description("if set to true, outputs to a new flowfile for debug purposes")
            .defaultValue("false")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    private final EventBus eventBus = new EventBus();
    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;
    private IoticsApi ioticsApi;

    private final Map<String, StreamObserver<ShareFeedDataRequest>> cache = new ConcurrentHashMap<>();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(DEBUG_FLAG);
        descriptors.add(IOTICS_HOST_SERVICE);
        descriptors = Collections.unmodifiableList(descriptors);

        relationships = new HashSet<>();
        relationships.add(SUCCESS);
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

        Boolean debugOn = context.getProperty(DEBUG_FLAG).asBoolean();

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            getLogger().warn("no flowfile found - not publishing");
            return;
        }

        CountDownLatch latch = new CountDownLatch(1);
        session.read(flowFile, in -> {
            try {
                JsonElement jsonElement = JsonParser.parseReader(new InputStreamReader(in));
                Gson gson = new Gson();
                List<MyTwin> receivedTwins;
                if (jsonElement.isJsonArray()) {
                    // Specify the list type using TypeToken
                    Type listType = new TypeToken<List<MyTwin>>() {}.getType();

                    // Convert the JsonElement to a List<MyCustomClass>
                    receivedTwins = gson.fromJson(jsonElement, listType);
                } else {
                    Type type = new TypeToken<MyTwin>() {}.getType();
                    MyTwin myTwin = gson.fromJson(jsonElement, type);
                    receivedTwins = Lists.newArrayList(myTwin);
                }

                // latch to # of streams, then pass in the output stream and dec in the
                // onnext or onerror to make sure we unblock when all sharing occurred

                receivedTwins.forEach(myTwin -> {
                    if (myTwin.keyName() != null) {
                        getLogger().warn("invalid twin. missing keyName: " + myTwin.id());
                    }

                    CountDownLatch latchFeeds = new CountDownLatch(myTwin.feeds().size());
                    myTwin.feeds().forEach(port -> eventBus.post(new StreamEvent(latchFeeds, myTwin, port)));

                    if (!debugOn) return;

                    // TODO: debug output on a downstream flowfile
                    FlowFile ff = session.create(flowFile);
                    try {
                        Type type = new TypeToken<MyTwin>() {
                        }.getType();
                        String json = gson.toJson(myTwin, type);
                        session.write(ff, out -> {
                            out.write(json.getBytes(StandardCharsets.UTF_8));
                        });
                        session.transfer(ff, SUCCESS);
                    } catch (Exception e) {
                        session.transfer(ff, FAILURE);
                    }
                });
                latch.countDown();
            } catch (Throwable t) {
                throw new ProcessException("error handling flowfile", t);
            }
        });
        try {
            latch.await();
            session.transfer(flowFile, SUCCESS);
        } catch (InterruptedException e) {
            session.transfer(flowFile, FAILURE);
            throw new RuntimeException(e);
        }
    }

    private void streamFeed(StreamEvent event) {
        String cacheKey = makeCacheKey(event);
        StreamObserver<ShareFeedDataRequest> stream = cache.computeIfAbsent(cacheKey, s -> newStream(event));
        if (stream == null) {
            return;
        }
        try {
            ShareFeedDataRequest request = newShareFeedDataRequest(event);
            if (request == null) {
                return;
            }
            stream.onNext(request);
        }catch (Exception e){
            stream.onError(e);
            // try again
            cache.remove(cacheKey);
            eventBus.post(event);
        }
    }

    private ShareFeedDataRequest newShareFeedDataRequest(StreamEvent event) {
        if(event.port().payloadAsJson() == null) {
            return null;
        }
        return ShareFeedDataRequest.newBuilder()
                .setHeaders(Builders.newHeadersBuilder(ioticsApi.getSim().agentIdentity().did()))
                .setArgs(ShareFeedDataRequest.Arguments.newBuilder()
                        .setFeedId(FeedID.newBuilder()
                                .setHostId(event.myTwin().hostDid())
                                .setTwinId(event.myTwin().id())
                                .setId (event.port().id())
                                .build())
                        .build())
                .setPayload(ShareFeedDataRequest.Payload.newBuilder()
                        .setSample(FeedData.newBuilder()
                                .setData(ByteString.copyFromUtf8(Objects.requireNonNull(event.port().payloadAsJson())))
                                .build())
                        .build())
                .build();
    }

    private StreamObserver<ShareFeedDataRequest> newStream(StreamEvent event) {
        try {
            return ioticsApi.feedAPIStub().streamFeedData(new StreamObserver<>() {
                @Override
                public void onNext(ShareFeedDataResponse shareFeedDataResponse) {
                    getLogger().info("shared: " + shareFeedDataResponse);
                }

                @Override
                public void onError(Throwable throwable) {
                    getLogger().error("error sharing", throwable);
                    if(throwable instanceof StatusRuntimeException) {
                        StatusRuntimeException ex = (StatusRuntimeException)throwable;
                        if(ex.getStatus().getCode() == Status.Code.PERMISSION_DENIED) {

                        }
                    }
                }

                @Override
                public void onCompleted() {

                }
            });
        } catch (Exception e) {
            getLogger().warn("unable to create stream", e);
            return null;
        }
    }

    private String makeCacheKey(StreamEvent event) {
        return event.myTwin().hostDid() + "/" + event.myTwin().id() + "/" + event.port().id();
    }

    public record StreamEvent(CountDownLatch latchFeeds, MyTwin myTwin, Port port) {
    }

    public class StreamNewFeedListener {

        @Subscribe
        public void onFollowEvent(IoticsPublisher.StreamEvent event) {
            streamFeed(event);
        }
    }
}
