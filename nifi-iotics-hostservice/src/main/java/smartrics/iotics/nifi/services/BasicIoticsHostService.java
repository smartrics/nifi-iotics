package smartrics.iotics.nifi.services;

import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import smartrics.iotics.host.IoticsApi;
import smartrics.iotics.identity.SimpleIdentityManager;

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
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor API_EXECUTOR_THREADS = new PropertyDescriptor
            .Builder().name("apiExecutorThreads")
            .displayName("Number of Threads for the IOTICS API")
            .description("""
                    The gRPC api client requires an executor to dispatch threads for async ops.
                    This service uses a newFixedThreadPool executor and this setting decides how many threads this executor is configured with.
                                """)
            .required(true)
            .defaultValue("16")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor HOST_DNS = new PropertyDescriptor
            .Builder().name("hostDNS")
            .displayName("Host DNS")
            .description("The host dns, for example 'myhost.iotics.space'")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    private Configuration configuration;
    private ExecutorService executor;
    private IoticsApi ioticsApi;
    private SimpleIdentityManager sim;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        configuration = new Configuration(context);
        Iotics iotics = Iotics.Builder.newBuilder().withConfiguration(configuration).build();
        ioticsApi = iotics.api();
        sim = iotics.sim();
        executor = Executors.newFixedThreadPool(configuration.apiExecutorThreads());
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

    public SimpleIdentityManager getSimpleIdentityManager() {
        return sim;
    }

    @OnDisabled
    public void onDisabled() {
        // Called when the service is disabled, use it to teardown your service
        if (executor != null) {
            executor.shutdown();
        }
        if (ioticsApi != null) {
            ioticsApi.stop(Duration.ofMillis(1000));
        }
    }

    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        // Return the list of properties your service supports
        return Arrays.asList(HOST_DNS, SEED, AGENT_KEY, USER_KEY, TOKEN_DURATION, API_EXECUTOR_THREADS);
    }
}
