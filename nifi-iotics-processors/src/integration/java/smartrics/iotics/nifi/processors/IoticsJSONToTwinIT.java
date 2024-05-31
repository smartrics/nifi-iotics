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

import com.google.gson.Gson;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import smartrics.iotics.nifi.processors.objects.MyTwinModel;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static smartrics.iotics.nifi.processors.Constants.ID_PROP;
import static smartrics.iotics.nifi.processors.IoticsControllerServiceFactory.injectIoticsHostService;

public class IoticsJSONToTwinIT {

    private static final Gson GSON = new Gson();

    private static final String CONTENT = """
            {
              "properties": [
                {
                  "key": "http://schema.org/identifier",
                  "value": "1234567890",
                  "type": "StringLiteral"
                }
              ],
              "feeds": [
                {
                  "id": "status"
                }
              ]
            }
            """;
    private static final String INVALID_JSON = """
            { "properties": [ {
              "key": "http://data.iotics.com/nifi/isOperational",
              "value": "true",
              "type": "Literal",
              "dataType": "boolean"
            }, }""";
    private static final String NULL_VALUE_PROP_JSON = """
            { "properties": [ {
              "key": "http://data.iotics.com/nifi/isOperational",
              "value": null,
              "type": "Literal",
              "dataType": "boolean"
            } }""";

    private static final String MISSING_VALUE_PROP_JSON = """
            { "properties": [ {
              "key": "http://data.iotics.com/nifi/isOperational",
              "type": "Literal",
              "dataType": "boolean"
            } }""";

    private static final String EMPTY_KEY_PROP_JSON = """
            { "properties": [ {
              "key": "",
              "value": "something",
              "type": "Literal",
              "dataType": "boolean"
            } }""";

    private static final String MISSING_KEY_PROP_JSON = """
            { "properties": [ {
              "value": "something",
              "type": "Literal",
              "dataType": "boolean"
            } }""";

    private static final String NULL_KEY_PROP_JSON = """
            { "properties": [ {
              "key": null,
              "value": "something",
              "type": "Literal",
              "dataType": "boolean"
            } }""";

    private static final String MISSING_TYPE_PROP_JSON = """
            { "properties": [ {
              "key": "http://a.key",
              "value": "something",
              "dataType": "boolean"
            } }""";

    private static final String NULL_TYPE_PROP_JSON = """
            { "properties": [ {
              "key": "http://a.key",
              "value": "something",
              "type": null,
              "dataType": "boolean"
            } }""";

    private static final String INVALID_TYPE_PROP_JSON = """
            { "properties": [ {
              "key": "http://a.key",
              "value": "something",
              "type": "this could also be empty",
              "dataType": "boolean"
            } }""";

    private TestRunner testRunner;

    @BeforeEach
    public void init() throws Exception {
        testRunner = TestRunners.newTestRunner(IoticsJSONToTwin.class);
        injectIoticsHostService(testRunner);
    }

    @Test
    public void missingIdentifierTransitionsToFail() throws IOException {
        testRunner.setProperty(ID_PROP.getName(), "http://schema.org/missing_identifier");
        testRunner.enqueue(CONTENT);
        testRunner.run(1);
        //assert the input Q is empty and the flowfile is processed
        testRunner.assertQueueEmpty();
        List<MockFlowFile> failure = testRunner.getFlowFilesForRelationship(Constants.FAILURE);
        MockFlowFile outputFlowfile = failure.getFirst();
        String outputFlowfileContent = new String(testRunner.getContentAsByteArray(outputFlowfile));
        Gson gson = new Gson();
        Map<String, Object> json = (Map<String, Object>) gson.fromJson(outputFlowfileContent, Map.class);
        assertThat(json.get("error").toString(), is(equalTo("invalid twin: missing property http://schema.org/missing_identifier")));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            NULL_VALUE_PROP_JSON, MISSING_VALUE_PROP_JSON, EMPTY_KEY_PROP_JSON, MISSING_KEY_PROP_JSON, NULL_KEY_PROP_JSON, MISSING_TYPE_PROP_JSON, NULL_TYPE_PROP_JSON, INVALID_TYPE_PROP_JSON
    })
    public void wrongJsonTransitionsToFail(String badJsonString) {
        testRunner.enqueue(badJsonString);
        testRunner.run(1);
        //assert the input Q is empty and the flowfile is processed
        testRunner.assertQueueEmpty();
        List<MockFlowFile> failure = testRunner.getFlowFilesForRelationship(Constants.FAILURE);
        MockFlowFile outputFlowfile = failure.getFirst();
        String outputFlowfileContent = new String(testRunner.getContentAsByteArray(outputFlowfile));
        Gson gson = new Gson();
        Map<String, Object> json = (Map<String, Object>) gson.fromJson(outputFlowfileContent, Map.class);
        assertThat(json.get("error").toString(), containsString("MalformedJsonException"));
    }

    @Test
    public void invalidJsonTransitionsToFail() {
        testRunner.enqueue(INVALID_JSON);
        testRunner.run(1);
        //assert the input Q is empty and the flowfile is processed
        testRunner.assertQueueEmpty();
        List<MockFlowFile> failure = testRunner.getFlowFilesForRelationship(Constants.FAILURE);
        MockFlowFile outputFlowfile = failure.getFirst();
        String outputFlowfileContent = new String(testRunner.getContentAsByteArray(outputFlowfile));
        Gson gson = new Gson();
        Map<String, Object> json = (Map<String, Object>) gson.fromJson(outputFlowfileContent, Map.class);
        assertThat(json.get("error").toString(), containsString("MalformedJsonException"));
    }

    @Test
    public void successTransitionGetsTheDID() throws IOException {
        testRunner.enqueue(CONTENT);
        testRunner.run(1);
        //assert the input Q is empty and the flowfile is processed
        testRunner.assertQueueEmpty();
        List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(Constants.SUCCESS);
        MockFlowFile outputFlowfile = success.getFirst();
        String outputFlowfileContent = new String(testRunner.getContentAsByteArray(outputFlowfile));
        Gson gson = new Gson();
        Map<String, Object> json = (Map<String, Object>) gson.fromJson(outputFlowfileContent, Map.class);
        assertNotNull(json.get("hostId"));
        assertNotNull(json.get("id"));

        List<MockFlowFile> original = testRunner.getFlowFilesForRelationship(Constants.ORIGINAL);
        assertThat("Number of flowfiles in Original Queue is as expected (No of Flowfile = 1) ", original.size() == 1);
    }

    @Test
    public void originalTransitionRetrievesTheInputFlowFile() throws IOException {
        testRunner.enqueue(CONTENT);
        testRunner.run(1);
        //assert the input Q is empty and the flowfile is processed
        testRunner.assertQueueEmpty();
        List<MockFlowFile> original = testRunner.getFlowFilesForRelationship(Constants.ORIGINAL);
        assertThat(original.size(), is(1));

        String outputFlowfileContent = new String(testRunner.getContentAsByteArray(original.getFirst()));
        Gson gson = new Gson();
        MyTwinModel myModel = gson.fromJson(outputFlowfileContent, MyTwinModel.class);
        assertThat(myModel.properties().getFirst().value(), is(equalTo("1234567890")));
        System.out.println(outputFlowfileContent);

        List<MockFlowFile> successResults = testRunner.getFlowFilesForRelationship(Constants.SUCCESS);
        assertThat(successResults.size(), is(1));
        outputFlowfileContent = new String(testRunner.getContentAsByteArray(successResults.getFirst()));
        System.out.println(outputFlowfileContent);
    }

}
