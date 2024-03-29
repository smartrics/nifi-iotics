package smartrics.iotics.nifi.services;

import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import smartrics.iotics.space.grpc.IoticsApi;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BasicIoticsHostService extends AbstractControllerService implements IoticsHostService {
    public static final PropertyDescriptor SEED = new org.apache.nifi.components.PropertyDescriptor
            .Builder().name("seed")
            .displayName("Seed")
            .description("The secret seed (this is a prototype - don't use it like this)")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor AGENT_KEY = new PropertyDescriptor
            .Builder().name("agentKey")
            .displayName("Agent Key")
            .description("The name of the key for this agent")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor USER_KEY = new PropertyDescriptor
            .Builder().name("userKey")
            .displayName("User Key")
            .description("The name of the key for this user")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor TOKEN_DURATION = new PropertyDescriptor
            .Builder().name("tokenDuration")
            .displayName("Token Duration in Seconds")
            .description("IOTICS API Authentication token duration in Seconds")
            .required(true)
            .defaultValue("3600")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor SPACE_DNS = new PropertyDescriptor
            .Builder().name("spaceDNS")
            .displayName("Space DNS")
            .description("The space dns, for example 'myspace.iotics.space'")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    private Configuration configuration;
    private ExecutorService executor;
    private IoticsApi ioticsApi;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        configuration = new Configuration(context);
        try {
            ioticsApi = Tools.newIoticsApi(configuration);
            executor = Executors.newFixedThreadPool(16);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public ExecutorService getExecutor() {
        return executor;
    }

    public IoticsApi getIoticsApi() {
        return ioticsApi;
    }

    @OnDisabled
    public void onDisabled() {
        // Called when the service is disabled, use it to teardown your service
        if(executor!=null) {
            executor.shutdown();
        }
        if(ioticsApi!=null) {
            ioticsApi.stop(Duration.ofMillis(1000));
        }
    }

    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        // Return the list of properties your service supports
        return Arrays.asList(SPACE_DNS, SEED, AGENT_KEY, USER_KEY, TOKEN_DURATION);
    }
}
