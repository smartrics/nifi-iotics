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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static smartrics.iotics.nifi.processors.Constants.ID_PROP;
import static smartrics.iotics.nifi.processors.IoticsControllerServiceFactory.injectIoticsHostService;

public class IoticsJSONLDToTwinIT {

    private TestRunner testRunner;

    @BeforeEach
    public void init() throws Exception {
        testRunner = TestRunners.newTestRunner(IoticsJSONLDToTwin.class);
        injectIoticsHostService(testRunner);
    }

    @Test
    public void missingIdentifierTransitionsToFail() throws IOException {
        testRunner.enqueue("""
{
  "@context": {
    "label": "rdfs:label"
  },
  "@type": "http://schema.org/Car",
  "label": "Toyota Camry 1"
}
""");
        testRunner.run(1);
        //assert the input Q is empty and the flowfile is processed
        testRunner.assertQueueEmpty();
        testRunner.setProperty(ID_PROP.getName(), "http://schema.org/identifier");
        List<MockFlowFile> failure = testRunner.getFlowFilesForRelationship(Constants.FAILURE);
//        assertThat("Number of flowfiles in Fail Queue is as expected (No of Flowfile = 1) ", failure.size() == 1);
        MockFlowFile outputFlowfile = failure.getFirst();
        String outputFlowfileContent = new String(testRunner.getContentAsByteArray(outputFlowfile));
        Gson gson = new Gson();
        Map<String, Object> json = (Map<String, Object>)gson.fromJson(outputFlowfileContent, Map.class);
        assertThat(json.get("error").toString(), is(equalTo("invalid JSON-LD: missing 'http://schema.org/identifier'")));
    }

    @Test
    public void invalidJsonTransitionsToFail() throws IOException {
        testRunner.enqueue("""
{
  "@context": {
    "label": "rdfs:label",
  },
}
""");
        testRunner.run(1);
        //assert the input Q is empty and the flowfile is processed
        testRunner.assertQueueEmpty();
        List<MockFlowFile> failure = testRunner.getFlowFilesForRelationship(Constants.FAILURE);
//        assertThat("Number of flowfiles in Fail Queue is as expected (No of Flowfile = 1) ", failure.size() == 1);
        MockFlowFile outputFlowfile = failure.getFirst();
        String outputFlowfileContent = new String(testRunner.getContentAsByteArray(outputFlowfile));
        Gson gson = new Gson();
        Map<String, Object> json = (Map<String, Object>)gson.fromJson(outputFlowfileContent, Map.class);
        assertThat(json.get("error").toString(), containsString("invalid JSON-LD: Unexpected character"));
    }

    @Test
    public void successTransitionGetsTheDID() throws IOException {
        String content = Files.readString(Path.of("src\\test\\resources\\car_twin.json"));
        testRunner.enqueue(content);
        testRunner.run(1);
        //assert the input Q is empty and the flowfile is processed
        testRunner.assertQueueEmpty();
        List<MockFlowFile> success = testRunner.getFlowFilesForRelationship(Constants.SUCCESS);
        MockFlowFile outputFlowfile = success.getFirst();
        String outputFlowfileContent = new String(testRunner.getContentAsByteArray(outputFlowfile));
        Gson gson = new Gson();
        Map<String, Object> json = (Map<String, Object>)gson.fromJson(outputFlowfileContent, Map.class);
        assertNotNull(json.get("hostId"));
        assertNotNull(json.get("id"));

        List<MockFlowFile> original = testRunner.getFlowFilesForRelationship(Constants.ORIGINAL);
        assertThat("Number of flowfiles in Original Queue is as expected (No of Flowfile = 1) ", original.size() == 1);


    }
    @Test
    public void originalTransitionRetrievesTheInputFlowFile() throws IOException {
        String content = Files.readString(Path.of("src\\test\\resources\\car_twin.json"));
        testRunner.enqueue(content);
        testRunner.run(1);
        //assert the input Q is empty and the flowfile is processed
        testRunner.assertQueueEmpty();
        List<MockFlowFile> original = testRunner.getFlowFilesForRelationship(Constants.ORIGINAL);
        assertThat("Number of flowfiles in Original Queue is as expected (No of Flowfile = 1) ", original.size() == 1);

        MockFlowFile outputFlowfile = original.getFirst();
        String outputFlowfileContent = new String(testRunner.getContentAsByteArray(outputFlowfile));
        Gson gson = new Gson();
        Map<String, Object> json = (Map<String, Object>)gson.fromJson(outputFlowfileContent, Map.class);

        assertThat(json.get("ID").toString(), is(equalTo("1")));
    }

}
