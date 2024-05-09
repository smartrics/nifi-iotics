package smartrics.iotics.nifi.processors;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static smartrics.iotics.nifi.processors.IoticsControllerServiceFactory.injectIoticsHostService;

public class IoticsFinderIT {
    private TestRunner testRunner;

    @BeforeEach
    public void init() throws Exception {
        testRunner = TestRunners.newTestRunner(IoticsFinder.class);
        injectIoticsHostService(testRunner);

        testRunner.setProperty(IoticsFinder.EXPIRY_TIMEOUT, "10");
        testRunner.setProperty(IoticsFinder.LOCATION, "{ 'r': 5, 'lat': 52.568213, 'lon': -0.244837 }");
//        testRunner.setProperty(IoticsFinder.TEXT, "");
//        testRunner.setProperty(IoticsFinder.PROPERTIES);
        testRunner.setProperty(Constants.QUERY_SCOPE, "LOCAL");
    }


    @Test
    public void testProcessor() {
        testRunner.run(1);
        //assert the input Q is empty and the flowfile is processed
        testRunner.assertQueueEmpty();
        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(Constants.SUCCESS);

        if (!results.isEmpty()) {
            results.forEach(mockFlowFile -> {
                String outputFlowfileContent = new String(testRunner.getContentAsByteArray(mockFlowFile));
                System.out.println(outputFlowfileContent);
            });
        } else {
            System.out.println("NOTHING BACK");
        }
    }

}
