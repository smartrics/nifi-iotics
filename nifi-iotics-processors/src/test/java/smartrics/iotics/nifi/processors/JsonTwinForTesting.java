package smartrics.iotics.nifi.processors;

import com.google.common.util.concurrent.ListenableFuture;
import com.iotics.api.TwinID;
import com.iotics.api.UpsertTwinResponse;
import smartrics.iotics.host.IoticsApi;
import smartrics.iotics.identity.Identity;
import smartrics.iotics.identity.IdentityManager;
import smartrics.iotics.nifi.processors.objects.JsonTwin;
import smartrics.iotics.nifi.processors.objects.MyTwinModel;
import smartrics.iotics.nifi.services.BasicIoticsHostService;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class JsonTwinForTesting extends JsonTwin {

    public JsonTwinForTesting(IoticsApi api, IdentityManager sim, Identity myIdentity, MyTwinModel model) {
        super(api, sim, myIdentity, model);
    }

    public static JsonTwinForTesting makeMyTwin(BasicIoticsHostService service) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        String content = Files.readString(Path.of("src\\test\\resources\\twin_with_feed.json"));
        MyTwinModel myTwin = MyTwinModel.fromJson(content);
        Identity twinIdentity = service.getSimpleIdentityManager().newTwinIdentityWithControlDelegation(myTwin.id(), "#master");
        JsonTwinForTesting myTwinMaker = new JsonTwinForTesting(service.getIoticsApi(), service.getSimpleIdentityManager(), twinIdentity, myTwin);
        ListenableFuture<UpsertTwinResponse> res = myTwinMaker.upsert();
        AtomicReference<TwinID> ref = new AtomicReference<>();
        res.addListener(() -> {
            try {
                TwinID twinID = res.resultNow().getPayload().getTwinId();
                ref.set(twinID);
            } catch (Exception e) {
                res.exceptionNow().printStackTrace();
            }
            latch.countDown();
        }, service.getExecutor());
        latch.await();
        TwinID twinID = ref.get();
        if (twinID == null) {
            throw new IllegalStateException("operation not completed - twinID is null");
        }
        MyTwinModel model = new MyTwinModel(twinID.getHostId(), twinID.getId(), myTwin.properties(), myTwin.feeds(), myTwin.inputs());
        return new JsonTwinForTesting(service.getIoticsApi(), service.getSimpleIdentityManager(), twinIdentity, model);
    }

    public void updatePayload() {
        super.getModel().feeds().forEach(f -> f.setShares(Map.of("data", UUID.randomUUID().toString())));
    }
}
