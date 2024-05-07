package smartrics.iotics.nifi.services;

import org.jetbrains.annotations.NotNull;
import smartrics.iotics.host.HostEndpoints;
import smartrics.iotics.host.HttpServiceRegistry;
import smartrics.iotics.host.IoticsApi;
import smartrics.iotics.host.IoticsApiImpl;
import smartrics.iotics.host.grpc.HostConnection;
import smartrics.iotics.host.grpc.HostConnectionImpl;
import smartrics.iotics.identity.SimpleConfig;
import smartrics.iotics.identity.SimpleIdentityImpl;
import smartrics.iotics.identity.SimpleIdentityManager;
import smartrics.iotics.identity.jna.JnaSdkApiInitialiser;
import smartrics.iotics.identity.jna.OsLibraryPathResolver;
import smartrics.iotics.identity.jna.SdkApi;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;

public final class Tools {

    @NotNull
    public static IoticsApi newIoticsApi(SimpleIdentityManager sim, String grpcEndpoint, Duration tokenDuration) throws IOException {
        HostConnection connection = new HostConnectionImpl(grpcEndpoint, sim, tokenDuration);
        return new IoticsApiImpl(connection);
    }

    @NotNull
    public static SimpleIdentityManager newSimpleIdentityManager(Configuration conf, String resolver) throws FileNotFoundException {
        SimpleConfig agentConf = new SimpleConfig(conf.seed(), conf.agentKey(), "#id-" + conf.agentKey().hashCode());
        SimpleConfig userConf = new SimpleConfig(conf.seed(), conf.userKey(), "#id-" + conf.userKey().hashCode());
        OsLibraryPathResolver pathResolver = new OsLibraryPathResolver() {
        };
        SdkApi api = new JnaSdkApiInitialiser("./lib", pathResolver).get();
        return SimpleIdentityManager.Builder.anIdentityManager()
                .withSimpleIdentity(new SimpleIdentityImpl(api, resolver, userConf.seed(), agentConf.seed()))
                .withAgentKeyID(agentConf.keyId())
                .withUserKeyID(userConf.keyId())
                .withAgentKeyName(agentConf.keyName())
                .withUserKeyName(userConf.keyName())
                .build();
    }


}
