package smartrics.iotics.nifi.services;

import org.jetbrains.annotations.NotNull;
import smartrics.iotics.identity.SimpleConfig;
import smartrics.iotics.space.HttpServiceRegistry;
import smartrics.iotics.space.IoticSpace;
import smartrics.iotics.space.grpc.IoticsApi;

import java.io.IOException;
import java.time.Duration;

public final class Tools {
    @NotNull
    public static IoticsApi newIoticsApi(Configuration conf) throws IOException {
        HttpServiceRegistry sr = new HttpServiceRegistry(conf.host());
        IoticSpace ioticSpace = new IoticSpace(sr);
        ioticSpace.initialise();

        SimpleConfig agentConf = new SimpleConfig(conf.seed(), conf.agentKey(), "#id-" + conf.agentKey().hashCode());
        SimpleConfig userConf = new SimpleConfig(conf.seed(), conf.userKey(), "#id-" + conf.userKey().hashCode());

        return new IoticsApi(ioticSpace, userConf, agentConf, Duration.ofSeconds(conf.tokenDuration()));
    }

}
