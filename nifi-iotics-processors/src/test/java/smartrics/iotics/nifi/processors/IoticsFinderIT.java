package smartrics.iotics.nifi.processors;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static smartrics.iotics.nifi.processors.IoticsControllerServiceFactory.injectIoticsHostService;

public class IoticsFinderIT {
    private TestRunner testRunner;

    @BeforeEach
    public void init() throws Exception {
        testRunner = TestRunners.newTestRunner(IoticsFinder.class);
        injectIoticsHostService(testRunner);

        testRunner.setProperty(IoticsFinder.EXPIRY_TIMEOUT, "3");
//        testRunner.setProperty(IoticsFinder.LOCATION, "{ 'r': 5, 'lat': 52.568213, 'lon': -0.244837 }");
//        testRunner.setProperty(IoticsFinder.TEXT, "Toyota");
        testRunner.setProperty(IoticsFinder.PROPERTIES, """
                [
                {"key": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "uri":"http://schema.org/Car"}
                ]
                """);
        testRunner.setProperty(Constants.QUERY_SCOPE, "LOCAL");
    }

    @Test
    public void testProcessorWithInputFlow() {
        testRunner.enqueue("""
                {
                    "expiryTimeout": "2",
                    "text": "something to search",
                    "properties": [
                        {
                            "key": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                            "uri":"http://schema.org/FooBarBaz"
                        }
                    ],
                    "location": {
                        "lat": 9876,
                        "lon": 0.987654,
                        "r": 1
                    },
                    "scope": "LOCAL",
                    "responseType": "MINIMAL"
                }
                """);
        RuntimeException thrown = assertThrows(RuntimeException.class, this::run);
        assertThat(thrown.getMessage(), is(equalTo("Nothing back!")));
    }

    @Test
    public void testProcessorNoInputFlow() {
        run();
    }


    void run() {
        testRunner.run(1);
        //assert the input Q is empty and the flowfile is processed
        testRunner.assertQueueEmpty();
        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(Constants.SUCCESS);

        if (!results.isEmpty()) {
            results.forEach(mockFlowFile -> {
                String json = new String(testRunner.getContentAsByteArray(mockFlowFile));
                JsonObject root = JsonParser.parseString(json).getAsJsonObject();
                assertTrue(root.has("id"));
                assertTrue(root.has("hostDid"));
            });
        } else {
            throw new RuntimeException("Nothing back!");
        }

    }
}
