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

import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.core.RDFDataset;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.iotics.api.TwinID;
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
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jetbrains.annotations.NotNull;
import smartrics.iotics.host.IoticsApi;
import smartrics.iotics.identity.Identity;
import smartrics.iotics.identity.SimpleIdentityManager;
import smartrics.iotics.nifi.processors.tools.AllowListEntryValidator;
import smartrics.iotics.nifi.processors.objects.JsonLdTwin;
import smartrics.iotics.nifi.services.IoticsHostService;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static smartrics.iotics.nifi.processors.Constants.*;

@Tags({"IOTICS", "TWIN CREATOR"})
@CapabilityDescription("""
Transforms a JSON-LD object into a twin. It's meant to be used using flow files outputted by the JOLT processor and the JSON-LD should be compatible with the shape of an IOTICS twin.
In practice, it needs to be a key-value map with no complex objects as values.

In order to determine the twin identity, the JSON-LD is expected to have an attribute with type http://schema.org/identifier. To determine the string used to create the identity, the scheme is removed from the IRI and used as a key name in the IOTICS Identity API.
""")
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class IoticsJSONLDToTwin extends AbstractProcessor {

    public static PropertyDescriptor ID_PROP = new PropertyDescriptor
            .Builder().name("idProp")
            .displayName("ID property")
            .description("This property should be present in the input flow file to identify the value used to determine the twin Identity. This value is then passed to the Identity library as KeyName.")
            .required(true)
            .defaultValue("http://schema.org/identifier")
            .addValidator(StandardValidators.URI_VALIDATOR)
            .build();

    public static PropertyDescriptor DEFAULT_ALLOW_LIST_PROP = new PropertyDescriptor
            .Builder().name("allowListProperty")
            .displayName("Allow Remote Access")
            .description("Specify whether this twin is visible remotely or not. " +
                    "Allowed values 'http://data.iotics.com/public#all', 'http://data.iotics.com/public#none', or a list of host DIDs separate by comma")
            .required(true)
            .defaultValue("http://data.iotics.com/public#none")
            .addValidator(new AllowListEntryValidator())
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
        descriptors.add(DEFAULT_ALLOW_LIST_PROP);
        descriptors.add(IOTICS_HOST_SERVICE);
        descriptors = Collections.unmodifiableList(descriptors);

        relationships = new HashSet<>();
        relationships.add(ORIGINAL);
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


    private ListenableFuture<UpsertTwinResponse> processFlow(
            final ProcessContext context,List<RDFDataset.Quad> quads) {
        // map keys
        String allowListProp = context.getProperty(DEFAULT_ALLOW_LIST_PROP).getValue();

        final String idPropValue = context.getProperty(ID_PROP).getValue();
        Optional<RDFDataset.Quad> res = quads.stream().filter(quad ->
                quad.getPredicate().getValue().equals(idPropValue)).findFirst();

        if(res.isEmpty()) {
            return exceptionFuture("invalid JSON-LD: missing '" + idPropValue + "'");
        }
        String twinIdentifier = res.get().getObject().getValue();
        Identity myIdentity = sim.newTwinIdentityWithControlDelegation(twinIdentifier, "#masterKey");
        JsonLdTwin twin = new JsonLdTwin(getLogger(), ioticsApi, sim, quads, myIdentity,allowListProp);
        return twin.upsert();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        IoticsHostService ioticsHostService =
                context.getProperty(IOTICS_HOST_SERVICE).asControllerService(IoticsHostService.class);

        this.ioticsApi = ioticsHostService.getIoticsApi();
        this.sim = ioticsHostService.getSimpleIdentityManager();
        this.executor = ioticsHostService.getExecutor();

        final AtomicReference<Throwable> error = new AtomicReference<>();
        final AtomicReference<TwinID> twinID = new AtomicReference<>();
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            // return - nothing to try again
            return;
        }

        final CountDownLatch waitForCompletion = new CountDownLatch(1);
        session.read(flowFile, in -> {
            ListenableFuture<UpsertTwinResponse> fut = processFlow(context, in);
            fut.addListener(() -> {
                try {
                    twinID.set(fut.resultNow().getPayload().getTwinId());
                } catch (IllegalStateException e) {
                    error.set(fut.exceptionNow());
                }
                waitForCompletion.countDown();
            }, executor);
        });
        try {
            waitForCompletion.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            getLogger().error("Timed out while waiting for result.", e);
            throw new ProcessException("Timed out while waiting for result.", e);
        }

        if(error.get() != null) {
            session.write(flowFile, out -> out.write(new Gson().toJson(
                    Map.of("error", error.get().getMessage()))
                    .getBytes(StandardCharsets.UTF_8)));
            session.transfer(flowFile, FAILURE);
        } else {
            if(twinID.get() != null) {
                FlowFile success = session.create(flowFile);
                session.write(success, out -> out.write(new Gson().toJson(
                        Map.of("hostId", twinID.get().getHostId(),
                                "id",twinID.get().getId()))
                        .getBytes(StandardCharsets.UTF_8)));
                session.transfer(success, SUCCESS);
            }
            session.transfer(flowFile, ORIGINAL);
        }
    }

    private ListenableFuture<UpsertTwinResponse> processFlow(ProcessContext context, InputStream in) throws IOException {
        JsonLdOptions options = new JsonLdOptions();
        RDFDataset dataset;
        try {
            Object jsonObject = JsonUtils.fromInputStream(in);
            // Convert JSON-LD to RDF triples
            dataset = (RDFDataset) JsonLdProcessor.toRDF(jsonObject, options);
        } catch (Exception e) {
            return exceptionFuture("invalid JSON-LD: " + e.getMessage());
        }
        Iterator<String> gIt = dataset.keySet().iterator();
        if(!gIt.hasNext()) {
            return exceptionFuture("invalid JSON-LD: missing graph");
        }
        String defaultGraph = gIt.next();
        List<RDFDataset.Quad> quads = dataset.getQuads(defaultGraph);

        return processFlow(context, quads);
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

    private static @NotNull SettableFuture<UpsertTwinResponse> exceptionFuture(String message) {
        SettableFuture<UpsertTwinResponse> f = SettableFuture.create();
        f.setException(new IllegalArgumentException(message));
        return f;
    }

}
