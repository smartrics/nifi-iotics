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
import com.google.gson.*;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.iotics.api.*;
import io.grpc.stub.StreamObserver;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import smartrics.iotics.nifi.processors.objects.MyTwin;
import smartrics.iotics.nifi.processors.objects.MyTwinList;
import smartrics.iotics.nifi.processors.tools.JsonToProperty;
import smartrics.iotics.nifi.processors.tools.LocationValidator;
import smartrics.iotics.nifi.services.IoticsHostService;
import smartrics.iotics.space.Builders;
import smartrics.iotics.space.grpc.IoticsApi;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.processor.util.StandardValidators.NON_BLANK_VALIDATOR;
import static org.apache.nifi.processor.util.StandardValidators.POSITIVE_INTEGER_VALIDATOR;
import static smartrics.iotics.nifi.processors.Constants.*;

@Tags({"IOTICS", "DIGITAL TWIN", "SEARCH"})
@CapabilityDescription("""
        Processor for IOTICS search
        """)
public class IoticsFinder extends AbstractProcessor {


    public static PropertyDescriptor EXPIRY_TIMEOUT = new PropertyDescriptor
            .Builder().name("expiryTimeoutSec")
            .displayName("Expiry Timeout in Seconds")
            .description("How long to wait for before disconnecting from receiving search results, in seconds")
            .required(true)
            .addValidator(POSITIVE_INTEGER_VALIDATOR)
            .build();
    public static PropertyDescriptor TEXT = new PropertyDescriptor
            .Builder().name("textFilter")
            .displayName("Text Filter")
            .description("text filter matching label or comment")
            .required(false)
            .addValidator(NON_BLANK_VALIDATOR)
            .build();
    public static PropertyDescriptor LOCATION = new PropertyDescriptor
            .Builder().name("locationFilter")
            .displayName("Location Filter")
            .description("JSON map with the following three keys: 'r', 'lat', 'lon', 'r' is the radius in KM of the circle centered in lat/lon.")
            .required(false)
            .addValidator(new LocationValidator())
            .build();
    public static PropertyDescriptor PROPERTIES = new PropertyDescriptor
            .Builder().name("propertiesFilter")
            .displayName("Properties Filter")
            .description("JSON array where each entry is a JSON map with 'key' and one of 'uri', 'stringLiteral', 'literal'. In case 'literal' is specified, an optional 'dataType' may be supplier")
            .required(false)
            .addValidator(NON_BLANK_VALIDATOR)
            .build();
    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;
    private IoticsApi ioticsApi;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(QUERY_SCOPE);
        descriptors.add(EXPIRY_TIMEOUT);
        descriptors.add(LOCATION);
        descriptors.add(TEXT);
        descriptors.add(PROPERTIES);
        descriptors.add(IOTICS_HOST_SERVICE);
        descriptors = Collections.unmodifiableList(descriptors);

        relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        relationships = Collections.unmodifiableSet(relationships);
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

        // TODO: get the search object from the flowfile
        //  FlowFile flowFile = session.get();

        String location = context.getProperty(LOCATION).getValue();
        JsonObject locationJson = null;
        if (location != null) {
            locationJson = JsonParser.parseString(location).getAsJsonObject();
        }
        String text = context.getProperty(TEXT).getValue();
        Duration expTo = Duration.ofSeconds(context.getProperty(EXPIRY_TIMEOUT).asInteger());
        String props = context.getProperty(PROPERTIES).getValue();
        JsonArray propsArray = null;
        if (props != null) {
            propsArray = JsonParser.parseString(props).getAsJsonArray();
        }
        Scope scope = Scope.valueOf(context.getProperty(QUERY_SCOPE).getValue());

        CountDownLatch latch = new CountDownLatch(1);
        try (ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1)) {
            // wait until the expiry timeout is done before terminating this trigger
            SearchRequest searchRequest = makeSearchRequest(locationJson, text, propsArray, scope, expTo);

            search(session, searchRequest);

            scheduler.schedule(latch::countDown, expTo.toSeconds(), TimeUnit.SECONDS);
            try {
                // latch unblocked by the scheduled task above
                latch.await();
            } catch (InterruptedException e) {
                throw new ProcessException(e);
            }
            scheduler.shutdown();
        }
    }

    private void search(ProcessSession session, SearchRequest searchRequest) {
        ioticsApi.searchAPIStub().synchronousSearch(searchRequest, new StreamObserver<>() {
            @Override
            public void onNext(SearchResponse searchResponse) {
                getLogger().info("Found twins [n={}]", searchResponse.getPayload().getTwinsList().size());
                // follow all feeds of all twins found
                ArrayList<SearchResponse.TwinDetails> list = Lists.newArrayList(searchResponse.getPayload().getTwinsList());
                if (list.isEmpty()) {
                    getLogger().info("No twins found matching your filters [filter={}]", searchRequest.getPayload().getFilter());
                    return;
                }
                MyTwinList to = new MyTwinList(searchResponse);
                to.twins().forEach(twin -> {
                    try {
                        Gson gson = new Gson();
                        FlowFile flowFile = session.create(session.get());
                        try {
                            String json = gson.toJson(twin, MyTwin.class);
                            session.write(flowFile, out -> out.write(json.getBytes(StandardCharsets.UTF_8)));
                            session.transfer(flowFile, SUCCESS);
                        } catch (Exception e) {
                            getLogger().warn("unable to parse to json {}", twin);
                            session.transfer(flowFile, FAILURE);
                        }
                    } catch(Exception e) {
                        getLogger().warn("unable to process twin {}", twin);
                    }
                });
            }

            @Override
            public void onError(Throwable throwable) {
                // TODO: catch the token expired and ignore on this
                getLogger().error("SEARCH ERR", throwable);
            }

            @Override
            public void onCompleted() {
                getLogger().warn("SEARCH COMPLETE");
            }
        });
    }

    private SearchRequest makeSearchRequest(JsonObject locationJson, String text, JsonArray propsArray, Scope scope, Duration exp) {
        SearchRequest.Payload.Filter.Builder filterBuilder = SearchRequest.Payload.Filter.newBuilder();
        if (locationJson != null) {
            filterBuilder.setLocation(GeoCircle.newBuilder()
                    .setRadiusKm(locationJson.get("r").getAsInt()) // search within 1Km
                    .setLocation(GeoLocation.newBuilder()
                            .setLat(locationJson.get("lat").getAsDouble())
                            .setLon(locationJson.get("lon").getAsDouble())
                            .build())
                    .build());
        }

        if (text != null) {
            filterBuilder.setText(StringValue.newBuilder().setValue(text).build());
        }
        if (propsArray != null) {
            propsArray.forEach(jsonElement -> filterBuilder
                    .addProperties(JsonToProperty.fromJson(jsonElement.getAsJsonObject())));
        }

        SearchRequest.Payload.Builder payloadBuilder = SearchRequest.Payload.newBuilder()
                .setResponseType(ResponseType.FULL)
                .setExpiryTimeout(Timestamp.newBuilder().setSeconds(exp.toSeconds()).build())
                .setFilter(filterBuilder);

        SearchRequest.Builder builder = SearchRequest.newBuilder()
                .setHeaders(Builders.newHeadersBuilder(this.ioticsApi.getSim().agentIdentity().did()))
                .setScope(scope)
                .setPayload(payloadBuilder);


        return builder.build();
    }
}
