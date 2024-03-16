package smartrics.iotics.nifi.processors;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockControllerServiceInitializationContext;
import org.apache.nifi.util.TestRunner;
import smartrics.iotics.nifi.services.BasicIoticsHostService;

import java.util.HashMap;
import java.util.Map;

public class IoticsControllerServiceFactory {

    public static void injectIoticsHostService(TestRunner testRunner) throws InitializationException {
        Map<String, String> conf = HashMap.newHashMap(5);

        conf.put(BasicIoticsHostService.SPACE_DNS.getName(), "demo.iotics.space");
        conf.put(BasicIoticsHostService.AGENT_KEY.getName(), "agentTestNifi1");
        conf.put(BasicIoticsHostService.USER_KEY.getName(), "userTestNifi1");
        conf.put(BasicIoticsHostService.SEED.getName(), "b34c09c9d21ad5f7535fac4c30afe1a9025f2caa1db92549044b1b0130d1ea49");
        conf.put(BasicIoticsHostService.TOKEN_DURATION.getName(), "30");

        BasicIoticsHostService ioticsHostService = new BasicIoticsHostService();
        MockControllerServiceInitializationContext siContext =
                new MockControllerServiceInitializationContext(ioticsHostService, "ioticsHostService");
        ioticsHostService.initialize(siContext);

        testRunner.setProperty(Constants.IOTICS_HOST_SERVICE, "ioticsHostService");
        // Register the mock service with the test runner
        testRunner.addControllerService("ioticsHostService", ioticsHostService, conf);
        testRunner.enableControllerService(ioticsHostService);
    }

}
