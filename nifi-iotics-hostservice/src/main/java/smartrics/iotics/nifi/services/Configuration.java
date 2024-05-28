package smartrics.iotics.nifi.services;

import org.apache.nifi.context.PropertyContext;

import java.util.Map;
import java.util.Optional;

import static smartrics.iotics.nifi.services.BasicIoticsHostService.*;

public record Configuration(String seed, String userKey, String agentKey,
                            String hostDNS, Integer tokenDuration,
                            Integer apiExecutorThreads) {


    public Configuration(Map<String, String> conf) {
        this(
                conf.get(SEED.getName()),
                conf.get(USER_KEY.getName()),
                conf.get(AGENT_KEY.getName()),
                conf.get(HOST_DNS.getName()),
                Integer.parseInt(conf.get(TOKEN_DURATION.getName())),
                Integer.parseInt(Optional.ofNullable(conf.get(API_EXECUTOR_THREADS.getName())).orElse("16"))
        );
    }

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
