package smartrics.iotics.nifi.processors;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static smartrics.iotics.nifi.processors.IoticsControllerServiceFactory.injectIoticsHostService;

public class IoticsPublisherIT {
    private TestRunner testRunner;

    @BeforeEach
    public void init() throws Exception {
        testRunner = TestRunners.newTestRunner(IoticsPublisher.class);
        injectIoticsHostService(testRunner);
    }


    @Test
    public void testProcessor() throws IOException, InterruptedException {
        String content = Files.readString(Path.of("src\\test\\resources\\twin_with_feed.json"));
        testRunner.enqueue(content);
        testRunner.run(1);
        //assert the input Q is empty and the flowfile is processed
        testRunner.assertQueueEmpty();
        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(Constants.ORIGINAL);

        if (!results.isEmpty()) {
            results.forEach(mockFlowFile -> {
                String outputFlowfileContent = new String(testRunner.getContentAsByteArray(mockFlowFile));
                System.out.println(outputFlowfileContent);
            });
        } else {
            System.out.println("NOTHING BACK");
        }
        Thread.sleep(5000);
    }

}
