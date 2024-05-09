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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.iotics.api.UpsertTwinResponse;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.jetbrains.annotations.NotNull;
import smartrics.iotics.host.IoticsApi;
import smartrics.iotics.identity.Identity;
import smartrics.iotics.identity.SimpleIdentityManager;
import smartrics.iotics.nifi.processors.objects.ConcreteTwin;
import smartrics.iotics.nifi.services.IoticsHostService;

import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static smartrics.iotics.nifi.processors.Constants.*;

@Tags({"IOTICS", "TWIN CREATOR"})
@CapabilityDescription("""
        Simple test to create twins from a template in JSON.
        The JSON must have the following structure:

        { "data" : [ { <key-value-pairs> }, {}, ... ], "map": { <map of keys to ontology> } }
         
        "data" is an array of json objects, each <key-value-pair> object is a set of string keys mapping to a
        value of numeric, boolean or string type.

        the "map" contains the mapping between the key in the <key-value-pairs> and a URI.

        Each object is a twin with properties having the key equal to the URI and the value equal to the value in the object.
                
        """)
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class IoticsJSONToTwin extends AbstractProcessor {

    public static PropertyDescriptor ONT_PREFIX = new org.apache.nifi.components.PropertyDescriptor
            .Builder().name("ontPrefix")
            .displayName("Ontology Prefix")
            .description("The prefix to add to the keys if they're not already URIs")
            .required(true)
            .defaultValue("https://data.iotics.com/nifi")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static PropertyDescriptor ID_PROP = new PropertyDescriptor
            .Builder().name("idProperty")
            .displayName("ID Property")
            .description("property in the incoming flow file that defines the key name for this twin")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;
    private IoticsApi ioticsApi;
    private SimpleIdentityManager sim;
    private ExecutorService executor;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(ID_PROP);
        descriptors.add(ONT_PREFIX);
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


    private Optional<ListenableFuture<UpsertTwinResponse>> processJsonObject(
            final ProcessContext context,
            final JsonObject keysMap,
            final JsonObject dataObject) {
        // map keys
        JsonObject jsonObject = new JsonObject();
        dataObject.keySet().forEach(s -> {
            JsonElement key = null;
            if (keysMap != null) {
                key = keysMap.get(s);
            }
            JsonElement v = dataObject.get(s);
            if (key != null) {
                jsonObject.add(key.getAsString(), v);
            } else {
                jsonObject.add(s, v);
            }
        });

        // process
        String jsonIdProp = context.getProperty(ID_PROP).getValue();
        String keyName = jsonObject.get(jsonIdProp).getAsString();
        String ontPrefix = context.getProperty(ONT_PREFIX).getValue();
        getLogger().info("Processing JSON object with keyName " + keyName);
        if (keyName == null) {
            getLogger().warn("Failed to read json object ID property '" + jsonIdProp + "', skipping");
            return Optional.empty();
        } else {
            Identity myIdentity = sim.newTwinIdentity(keyName, "#masterKey");
            ConcreteTwin twin = new ConcreteTwin(getLogger(), ioticsApi, sim, jsonObject, ontPrefix, myIdentity);
            return Optional.of(twin.upsert());
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        IoticsHostService ioticsHostService =
                context.getProperty(IOTICS_HOST_SERVICE).asControllerService(IoticsHostService.class);

        this.ioticsApi = ioticsHostService.getIoticsApi();
        this.sim = ioticsHostService.getSimpleIdentityManager();
        this.executor = ioticsHostService.getExecutor();

        final AtomicReference<JsonObject> value = new AtomicReference<>();
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            // return - nothing to try again
            return;
        }

        session.read(flowFile, in -> {
            JsonArray successes = new JsonArray();
            JsonArray failures = new JsonArray();
            try {
                JsonElement jsonElement = JsonParser.parseReader(new InputStreamReader(in));
                JsonObject map = jsonElement.getAsJsonObject().getAsJsonObject("map");
                JsonArray dataArray = jsonElement.getAsJsonObject().getAsJsonArray("data");

                JsonArray data = new JsonArray();
                if (dataArray != null) {
                    data = dataArray.getAsJsonArray();
                }

                CountDownLatch latch = new CountDownLatch(data.size());
                data.asList()
                        .forEach(el -> processJsonObject(context, map, el.getAsJsonObject())
                                .ifPresentOrElse(f -> processFuture(f, latch, successes, failures), latch::countDown));
                latch.await();
                JsonObject o = new JsonObject();
                o.add("successes", successes);
                o.add("failures", failures);
                value.set(o);
            } catch (Exception ex) {
                getLogger().error("Failed to read json string.", ex);
                throw new ProcessException(ex.getMessage(), ex);
            }
        });

        // To write the results back out of flow file
        flowFile = session.write(flowFile, out -> out.write(value.get().toString().getBytes()));
        session.transfer(flowFile, SUCCESS);
    }

    private void processFuture(ListenableFuture<UpsertTwinResponse> f, CountDownLatch latch, JsonArray successes, JsonArray failures) {
        Futures.addCallback(f, new FutureCallback<>() {
            @Override
            public void onSuccess(UpsertTwinResponse result) {
                latch.countDown();
                String id = result.getPayload().getTwinId().getId();
                successes.add(id);
                getLogger().info("Processed successfully twin with did " + id);
            }

            @Override
            public void onFailure(@NotNull Throwable ex) {
                latch.countDown();
                failures.add(ex.getMessage());
                getLogger().warn("Processed unsuccessfully twin, message=" + ex.getMessage());
            }
        }, executor);
    }
}
