package smartrics.iotics.nifi.processors;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static smartrics.iotics.nifi.processors.IoticsControllerServiceFactory.injectIoticsHostService;

public class IoticsSPARQLQueryTest {
    private TestRunner testRunner;

    @BeforeEach
    public void init() throws Exception {
        testRunner = TestRunners.newTestRunner(IoticsSPARQLQuery.class);
        injectIoticsHostService(testRunner);
        String content = Files.readString(Path.of("src\\test\\resources\\query.sparql"));
        testRunner.setProperty(IoticsSPARQLQuery.SPARQL_QUERY, content);
        testRunner.setProperty(Constants.QUERY_SCOPE, "LOCAL");
    }

    //@Test
    public void testProcessor() {
        testRunner.run(1);
        //assert the input Q is empty and the flowfile is processed
        testRunner.assertQueueEmpty();
        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(Constants.SUCCESS);

        MockFlowFile outputFlowfile = results.getFirst();
        String outputFlowfileContent = new String(testRunner.getContentAsByteArray(outputFlowfile));

        System.out.println(outputFlowfileContent);
    }

}
