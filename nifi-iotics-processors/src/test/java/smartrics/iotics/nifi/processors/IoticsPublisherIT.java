package smartrics.iotics.nifi.processors;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.iotics.api.TwinID;
import com.iotics.api.UpsertTwinResponse;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import smartrics.iotics.identity.Identity;
import smartrics.iotics.nifi.processors.objects.*;
import smartrics.iotics.nifi.services.BasicIoticsHostService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static smartrics.iotics.nifi.processors.IoticsControllerServiceFactory.injectIoticsHostService;

public class IoticsPublisherIT {
    private TestRunner testRunner;
    private TwinID tID;
    private Port feed;

    @BeforeEach
    public void init() throws Exception {
        testRunner = TestRunners.newTestRunner(IoticsPublisher.class);
        BasicIoticsHostService service = injectIoticsHostService(testRunner);
        String content = Files.readString(Path.of("src\\test\\resources\\car_twin.json"));
        MyTwinModel model = MyTwinModel.fromJson(content);
        MyProperty idProp = model.findProperty("http://schema.org/identifier").orElseThrow();
        Identity id = service.getSimpleIdentityManager().newTwinIdentity(idProp.value(), "#master");
        JsonTwin twin = new JsonTwin(service.getIoticsApi(), service.getSimpleIdentityManager(), id, model);
        ListenableFuture<UpsertTwinResponse> op = twin.upsert();
        UpsertTwinResponse resp = op.get();
        tID = resp.getPayload().getTwinId();
        feed = model.feeds().getFirst();
    }


    @Test
    public void testProcessor() throws IOException, InterruptedException {
        int count = 5;
        String content = newRandomContent(count);

        testRunner.enqueue(content);
        testRunner.run();
        //assert the input Q is empty and the flowfile is processed
        testRunner.assertQueueEmpty();
        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(Constants.SUCCESS);

        assertThat(results.size(), is(count));
        results.forEach(mockFlowFile -> {
            String outputFlowfileContent = new String(testRunner.getContentAsByteArray(mockFlowFile));
            System.out.println(outputFlowfileContent);
        });

        results = testRunner.getFlowFilesForRelationship(Constants.ORIGINAL);
        assertThat(results.size(), is(1));
        String outputFlowfileContent = new String(testRunner.getContentAsByteArray(results.getFirst()));
        System.out.println(outputFlowfileContent);


        Thread.sleep(1000);
    }

    private String newRandomContent(int el) {
        Random r = new Random();
        List<MyTwinModel> twins = new ArrayList<>();
        for (int i = 0; i < el; i++) {
            String v = "" + r.nextBoolean();
            MyValue value = new MyValue(feed.values().getFirst().label(), null, null, v);
            Port f = new Port(feed.id(), List.of(), List.of(value), false);
            MyTwinModel m = new MyTwinModel(null, tID.getId(), List.of(), List.of(f), List.of());
            twins.add(m);
        }
        Gson g = new Gson();
        return g.toJson(twins);
    }

}
