package smartrics.iotics.nifi.processors;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static smartrics.iotics.nifi.processors.IoticsControllerServiceFactory.injectIoticsHostService;

public class IoticsSPARQLQueryIT {
    private TestRunner testRunner;

    @BeforeEach
    public void init() throws Exception {
        testRunner = TestRunners.newTestRunner(IoticsSPARQLQuery.class);
        injectIoticsHostService(testRunner);
        String content = Files.readString(Path.of("src\\test\\resources\\car_query.sparql"));
        testRunner.setProperty(IoticsSPARQLQuery.SPARQL_QUERY, content);
        testRunner.setProperty(Constants.QUERY_SCOPE, "LOCAL");
    }

    @Test
    public void testProcessor() {
        testRunner.run(1);
        //assert the input Q is empty and the flowfile is processed
        testRunner.assertQueueEmpty();
        List<MockFlowFile> resultsList = testRunner.getFlowFilesForRelationship(Constants.SUCCESS);

        MockFlowFile outputFlowfile = resultsList.getFirst();
        String json = new String(testRunner.getContentAsByteArray(outputFlowfile));

        JsonObject root = JsonParser.parseString(json).getAsJsonObject();
        JsonObject results = root.getAsJsonObject("results");
        JsonArray bindings = results.getAsJsonArray("bindings");

        for (int i = 0; i < bindings.size(); i++) {
            JsonObject binding = bindings.get(i).getAsJsonObject();
            JsonObject numberOfCarsNode = binding.getAsJsonObject("numberOfCars");
            if ("literal".equals(numberOfCarsNode.get("type").getAsString()) &&
                    "http://www.w3.org/2001/XMLSchema#integer".equals(numberOfCarsNode.get("datatype").getAsString())) {
                int numberOfCars = numberOfCarsNode.get("value").getAsInt();
                assertThat(numberOfCars, is(greaterThan(0)));
            }
        }
    }

}
