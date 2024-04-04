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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static smartrics.iotics.nifi.processors.IoticsControllerServiceFactory.injectIoticsHostService;

public class IoticsFollowerIT {
    private TestRunner testRunner;

    @BeforeEach
    public void init() throws Exception {
        testRunner = TestRunners.newTestRunner(IoticsFollower.class);
        injectIoticsHostService(testRunner);

        testRunner.setProperty(IoticsFollower.FOLLOWER_LABEL, "MyTestTwin");
        testRunner.setProperty(IoticsFollower.FOLLOWER_COMMENT, "My Test twin to check workings of find/bind in NiFi");
        testRunner.setProperty(IoticsFollower.FOLLOWER_CLASSIFIER, "http://schema.org/SoftwareApplication");
        testRunner.setProperty(IoticsFollower.FOLLOWER_ID, "MyTestTwin08976576879089");
    }


    @Test
    public void testProcessor() throws IOException, InterruptedException {
        // twins found by finder
        String content = Files.readString(Path.of("src\\test\\resources\\IoticsFinder.json"));

        testRunner.enqueue(content);
        testRunner.run(1);
        //assert the input Q is empty and the flowfile is processed
        testRunner.assertQueueEmpty();
        CountDownLatch latch = new CountDownLatch(10);

        try (ExecutorService executor = Executors.newFixedThreadPool(1)) {
            executor.submit(() -> {
                while(true) {
                    List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(Constants.SUCCESS);

                    MockFlowFile outputFlowfile = results.getFirst();
                    String outputFlowfileContent = new String(testRunner.getContentAsByteArray(outputFlowfile));
                    System.out.println(latch.getCount() + ") " + outputFlowfileContent);
                    latch.countDown();
                    if(latch.getCount() == 0) {
                        return;
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }

        latch.await();
    }

}
