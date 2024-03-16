package smartrics.iotics.nifi.services;

import org.apache.nifi.context.PropertyContext;

import java.time.Duration;

import static smartrics.iotics.nifi.services.BasicIoticsHostService.*;

public record Configuration(String seed, String userKey, String agentKey, String space, Integer tokenDuration) {


    public Configuration(PropertyContext context) {
        this(
                context.getProperty(SEED).getValue(),
                context.getProperty(USER_KEY).getValue(),
                context.getProperty(AGENT_KEY).getValue(),
                context.getProperty(SPACE_DNS).getValue(),
                context.getProperty(TOKEN_DURATION).asInteger()
        );
    }

}
