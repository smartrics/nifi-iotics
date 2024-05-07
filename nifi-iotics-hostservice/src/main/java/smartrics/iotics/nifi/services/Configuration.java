package smartrics.iotics.nifi.services;

import org.apache.nifi.context.PropertyContext;

import java.time.Duration;

import static smartrics.iotics.nifi.services.BasicIoticsHostService.*;

public record Configuration(String seed, String userKey, String agentKey,
                            String hostDNS, Integer tokenDuration,
                            Integer apiExecutorThreads) {


    public Configuration(PropertyContext context) {
        this(
                context.getProperty(SEED).getValue(),
                context.getProperty(USER_KEY).getValue(),
                context.getProperty(AGENT_KEY).getValue(),
                context.getProperty(HOST_DNS).getValue(),
                context.getProperty(TOKEN_DURATION).asInteger(),
                context.getProperty(API_EXECUTOR_THREADS).asInteger()
        );
    }

}
