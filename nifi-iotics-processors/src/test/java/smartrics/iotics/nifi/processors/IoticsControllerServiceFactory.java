package smartrics.iotics.nifi.processors;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockControllerServiceInitializationContext;
import org.apache.nifi.util.TestRunner;
import smartrics.iotics.nifi.services.BasicIoticsHostService;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class IoticsControllerServiceFactory {

    public static void injectIoticsHostService(TestRunner testRunner) throws InitializationException, IOException {
        Properties prop = new Properties();
        // The path to your properties file
        String propFileName = ".env";
        Path absolutePath = Paths.get(propFileName).toAbsolutePath();
        try (InputStream inputStream = new FileInputStream(absolutePath.toFile())) {
            prop.load(inputStream);
            Map<String, String> conf = HashMap.newHashMap(5);
            for (String key : prop.stringPropertyNames()) {
                conf.put(key, prop.getProperty(key));
            }
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


}
