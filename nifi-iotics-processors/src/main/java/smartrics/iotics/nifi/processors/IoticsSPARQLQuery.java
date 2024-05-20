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
import com.google.protobuf.ByteString;
import com.iotics.api.Scope;
import com.iotics.api.SparqlQueryRequest;
import com.iotics.api.SparqlQueryResponse;
import io.grpc.stub.StreamObserver;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import smartrics.iotics.host.Builders;
import smartrics.iotics.host.IoticsApi;
import smartrics.iotics.identity.SimpleIdentityManager;
import smartrics.iotics.nifi.services.IoticsHostService;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static smartrics.iotics.nifi.processors.Constants.*;

@Tags({"IOTICS", "SPARQL", "QUERY"})
@CapabilityDescription("""
        Runs a SPARQL query and returns the output to the fileflow.
        """)
@ReadsAttribute(attribute = "sql.query", description = "The SQL select query to execute.")
public class IoticsSPARQLQuery extends AbstractProcessor {
    public static PropertyDescriptor SPARQL_QUERY = new PropertyDescriptor
            .Builder().name("sparqlQuery")
            .displayName("SPARQL query")
            .description("the query that, when executed, it will find the twin and feed to follow")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();
    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;
    private IoticsApi ioticsApi;
    private SimpleIdentityManager sim;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(QUERY_SCOPE);
        descriptors.add(SPARQL_QUERY);
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
        this.sim = ioticsHostService.getSimpleIdentityManager();

        final FlowFile flowFile = session.create();



        String query = context.getProperty(SPARQL_QUERY).getValue();
        Scope scope = Scope.valueOf(context.getProperty(QUERY_SCOPE).getValue());

        CountDownLatch latch = new CountDownLatch(1);
        query(query, scope).thenAccept(queryResult -> {
            FlowFile updatedFlowFile = session.write(flowFile, out -> out.write(queryResult.getBytes()));
            session.transfer(updatedFlowFile, SUCCESS);
            latch.countDown();
        }).exceptionally(throwable -> {
            session.transfer(flowFile, FAILURE);
            return null;
        });

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new ProcessException(e);
        }
    }

    private CompletableFuture<String> query(String query, Scope scope) {
        List<ByteString> chunks = Lists.newArrayList();
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
