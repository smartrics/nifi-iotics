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

import com.google.protobuf.ByteString;
import com.iotics.api.Scope;
import com.iotics.api.SparqlQueryRequest;
import com.iotics.api.SparqlQueryResponse;
import io.grpc.stub.StreamObserver;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import smartrics.iotics.host.Builders;
import smartrics.iotics.host.IoticsApi;
import smartrics.iotics.identity.SimpleIdentityManager;
import smartrics.iotics.nifi.services.IoticsHostService;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static smartrics.iotics.nifi.processors.Constants.*;

@Tags({"IOTICS", "SPARQL", "QUERY"})
@CapabilityDescription("""
        Runs a SPARQL query and returns the output to the flow file.
        The SPARQL query is provided an input flow file and scope set as an attribute.
        """)
@SeeAlso(classNames = {"smartrics.iotics.nifi.processors.IoticsFinder"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@ReadsAttribute(attribute = "sparql.query", description = "The SPARQL query to execute.")
@WritesAttributes({
        @WritesAttribute(attribute = "sparql.query.result", description = "The result of the SPARQL query."),
        @WritesAttribute(attribute = "sparql.query.error", description = "Any error encountered during the SPARQL query execution.")
})
public class IoticsSPARQLQuery extends AbstractProcessor {

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;
    private IoticsApi ioticsApi;
    private SimpleIdentityManager sim;

    public static String readInputStream(InputStream inputStream) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.joining(System.lineSeparator()));
        }
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(QUERY_SCOPE);
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

        final FlowFile ff = session.get();

        if (ff == null) {
            return;
        }

        final AtomicReference<String> queryRef = new AtomicReference<>(ff.getAttribute("sparql.query"));

        if (queryRef.get() == null) {
            // Read the FlowFile content synchronously if not available in the attribute
            try {
                session.read(ff, in -> {
                    try {
                        String content = readInputStream(in);
                        queryRef.set(content);
                    } catch (IOException e) {
                        getLogger().error("Failed to read flow file content", e);
                        throw new ProcessException(e);
                    }
                });
            } catch (ProcessException e) {
                session.transfer(ff, FAILURE);
                return;
            }
        }

        // Transfer the original FlowFile immediately after reading its content
        session.transfer(ff, ORIGINAL);

        Scope scope = Scope.valueOf(context.getProperty(QUERY_SCOPE).getValue());

        // Create a new FlowFile for the query result
        FlowFile flowFile = session.create();

        // Perform the SPARQL query asynchronously
        CountDownLatch latch = new CountDownLatch(1);
        query(queryRef.get(), scope).thenAccept(queryResult -> {
            try {
                FlowFile updatedFlowFile = session.write(flowFile, out -> out.write(queryResult.getBytes()));
                updatedFlowFile = session.putAttribute(updatedFlowFile, "sparql.query.result", queryResult);
                session.transfer(updatedFlowFile, SUCCESS);
                latch.countDown();
            } catch (Exception e) {
                getLogger().error("Failed to write query result", e);
                session.remove(flowFile);
            }
        }).exceptionally(throwable -> {
            getLogger().error("Error during SPARQL query execution", throwable);
            FlowFile errorFlowFile = session.putAttribute(flowFile, "sparql.query.error", throwable.getMessage());
            session.transfer(errorFlowFile, FAILURE);
            return null;
        });

        try {
            // seems to be needed to have tests passing otherwise the session doesn't seem to be transferred
            latch.await();
        } catch (InterruptedException e) {
            throw new ProcessException(e);
        }
    }

    private CompletableFuture<String> query(String query, Scope scope) {
        List<ByteString> chunks = new ArrayList<>();
        CompletableFuture<String> resultFuture = new CompletableFuture<>();
        getLogger().debug("Running [" + scope + "] query: " + query);
        this.ioticsApi.metaAPI().sparqlQuery(SparqlQueryRequest.newBuilder()
                .setHeaders(Builders.newHeadersBuilder(sim.agentIdentity()))
                .setScope(scope)
                .setPayload(SparqlQueryRequest.Payload.newBuilder()
                        .setQuery(ByteString.copyFromUtf8(query))
                        .build())
                .build(), new StreamObserver<>() {
            @Override
            public void onNext(SparqlQueryResponse sparqlQueryResponse) {
                SparqlQueryResponse.Payload payload = sparqlQueryResponse.getPayload();
                getLogger().debug("Chunk: [seq={}, last={}, status={}]", payload.getSeqNum(), payload.getLast(), payload.getStatus());
                chunks.add(payload.getResultChunk());
                if (payload.getLast()) {
                    String joinedString = chunks.stream()
                            .map(ByteString::toStringUtf8)
                            .collect(Collectors.joining());
                    resultFuture.complete(joinedString);
                }
            }

            @Override
            public void onError(Throwable t) {
                getLogger().error("Error in SPARQL query response", t);
                resultFuture.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {
                getLogger().debug("Sparql response processing completed");
            }
        });
        return resultFuture;
    }
}
