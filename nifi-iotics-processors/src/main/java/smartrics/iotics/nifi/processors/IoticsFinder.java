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
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import smartrics.iotics.host.Builders;
import smartrics.iotics.host.IoticsApi;
import smartrics.iotics.identity.SimpleIdentityManager;
import smartrics.iotics.nifi.processors.objects.MyTwinModel;
import smartrics.iotics.nifi.processors.objects.MyTwinModelList;
import smartrics.iotics.nifi.processors.tools.JsonToProperty;
import smartrics.iotics.nifi.processors.tools.LocationValidator;
import smartrics.iotics.nifi.services.IoticsHostService;

import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
            .description("JSON array where each entry is a JSON map with 'key' and one of 'uri', 'stringLiteral', 'literal'. In case 'literal' is specified, an 'dataType' may be supplied, with value one of the valid xsd data types (int, boolean, anyURI, ...)")
            .required(false)
            .addValidator(NON_BLANK_VALIDATOR)
            .build();
    public static PropertyDescriptor QUERY_RESPONSE_TYPE = new PropertyDescriptor.Builder()
            .name("queryResponseType")
            .displayName("Query Response Type")
            .description("query response type: " + ResponseType.FULL.name() + ", " + ResponseType.LOCATED + ", " + ResponseType.MINIMAL)
            .allowableValues(Arrays.stream(ResponseType.values())
                    .map(enumValue -> new AllowableValue(enumValue.name(), enumValue.name()))
                    .toArray(AllowableValue[]::new))
            .required(true)
            .defaultValue(ResponseType.FULL.name())
            .build();
    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;
    private IoticsApi ioticsApi;
    private SimpleIdentityManager sim;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(QUERY_SCOPE);
        descriptors.add(QUERY_RESPONSE_TYPE);
        descriptors.add(EXPIRY_TIMEOUT);
        descriptors.add(LOCATION);
        descriptors.add(TEXT);
        descriptors.add(PROPERTIES);
        descriptors.add(IOTICS_HOST_SERVICE);
        descriptors = Collections.unmodifiableList(descriptors);

        relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(ORIGINAL);
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
        this.sim = ioticsHostService.getSimpleIdentityManager();

        String location = context.getProperty(LOCATION).getValue();
        AtomicReference<JsonObject> locationJson = new AtomicReference<>();
        if (location != null) {
            locationJson.set(JsonParser.parseString(location).getAsJsonObject());
        }
        AtomicReference<String> text = new AtomicReference<>(context.getProperty(TEXT).getValue());
        AtomicReference<Duration> expTo = new AtomicReference<>(Duration.ofSeconds(context.getProperty(EXPIRY_TIMEOUT).asInteger()));
        String props = context.getProperty(PROPERTIES).getValue();
        AtomicReference<JsonArray> propsArray = new AtomicReference<>();
        if (props != null) {
            propsArray.set(JsonParser.parseString(props).getAsJsonArray());
        }
        AtomicReference<Scope> scope = new AtomicReference<>(Scope.valueOf(context.getProperty(QUERY_SCOPE).getValue()));
        AtomicReference<ResponseType> respType = new AtomicReference<>(ResponseType.valueOf(context.getProperty(QUERY_RESPONSE_TYPE).getValue()));

        final CountDownLatch latch1 = new CountDownLatch(1);
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        session.read(flowFile, in -> {
            JsonElement jsonElement = JsonParser.parseReader(new InputStreamReader(in));
            JsonObject jsonObject = jsonElement.getAsJsonObject();
            if (jsonObject.has("text")) {
                text.set(jsonObject.get("text").getAsString());
            }
            if (jsonObject.has("location")) {
                locationJson.set(jsonObject.get("location").getAsJsonObject());
            }
            if (jsonObject.has("expiryTimeout")) {
                expTo.set(Duration.ofSeconds(jsonObject.get("expiryTimeout").getAsInt()));
            }
            if (jsonObject.has("scope")) {
                scope.set(Scope.valueOf(jsonObject.get("scope").getAsString()));
            }
            if (jsonObject.has("responseType")) {
                respType.set(ResponseType.valueOf(jsonObject.get("responseType").getAsString()));
            }
            if (jsonObject.has("properties")) {
                propsArray.set(jsonObject.get("properties").getAsJsonArray());
            }
            latch1.countDown();
        });

        try {
            latch1.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ProcessException("Interrupted whilst reading Flow file", e);
        }

        session.transfer(flowFile, ORIGINAL);

        CountDownLatch latch2 = new CountDownLatch(1);
        try (ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1)) {
            // wait until the expiry timeout is done before terminating this trigger
            SearchRequest searchRequest = makeSearchRequest(locationJson.get(), text.get(), propsArray.get(), respType.get(), scope.get(), expTo.get());

            search(session, searchRequest);

            scheduler.schedule(latch2::countDown, expTo.get().toSeconds(), TimeUnit.SECONDS);
            try {
                // latch unblocked by the scheduled task above
                latch2.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ProcessException("Interrupted whilst reading Flow file", e);
            }
            scheduler.shutdown();
        }
    }

    private void search(ProcessSession session, SearchRequest searchRequest) {
        ioticsApi.searchAPI().synchronousSearch(searchRequest, new StreamObserver<>() {
            @Override
            public void onNext(SearchResponse searchResponse) {
                // follow all feeds of all twins found
                ArrayList<SearchResponse.TwinDetails> list = Lists.newArrayList(searchResponse.getPayload().getTwinsList());
                if (list.isEmpty()) {
                    return;
                }
                MyTwinModelList to = new MyTwinModelList(searchResponse);
                to.twins().forEach(twin -> {
                    try {
                        Gson gson = new Gson();
                        FlowFile flowFile = session.create(session.get());
                        try {
                            String json = gson.toJson(twin, MyTwinModel.class);
                            session.write(flowFile, out -> out.write(json.getBytes(StandardCharsets.UTF_8)));
                            session.transfer(flowFile, SUCCESS);
                        } catch (Exception e) {
                            getLogger().warn("unable to parse to json {}", twin);
                            session.transfer(flowFile, FAILURE);
                        }
                    } catch (Exception e) {
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

    private SearchRequest makeSearchRequest(JsonObject locationJson, String text, JsonArray propsArray, ResponseType responseType, Scope scope, Duration exp) {
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
                .setResponseType(responseType)
                .setExpiryTimeout(Timestamp.newBuilder().setSeconds(exp.toSeconds()).build())
                .setFilter(filterBuilder);

        SearchRequest.Builder builder = SearchRequest.newBuilder()
                .setHeaders(Builders.newHeadersBuilder(this.sim.agentIdentity()))
                .setScope(scope)
                .setPayload(payloadBuilder);


        return builder.build();
    }
}
