package smartrics.iotics.nifi.services;

import com.google.common.base.Strings;
import smartrics.iotics.host.HostEndpoints;
import smartrics.iotics.host.HttpServiceRegistry;
import smartrics.iotics.host.IoticsApi;
import smartrics.iotics.identity.SimpleIdentityManager;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;

public record Iotics(SimpleIdentityManager sim, IoticsApi api, Configuration configuration,
                     HttpServiceRegistry registry, HostEndpoints endpoints) {

    public final static class Builder {
        private SimpleIdentityManager sim;
        private IoticsApi api;
        private Configuration configuration;
        private HttpServiceRegistry registry;
        private HostEndpoints endpoints;

        private Builder() {
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder withSim(SimpleIdentityManager sim) {
            this.sim = sim;
            return this;
        }

        public Builder withApi(IoticsApi api) {
            this.api = api;
            return this;
        }

        public Builder withConfiguration(Configuration configuration) {
            this.configuration = configuration;
            return this;
        }

        public Builder withRegistry(HttpServiceRegistry registry) {
            this.registry = registry;
            return this;
        }

        public Builder withEndpoints(HostEndpoints endpoints) {
            this.endpoints = endpoints;
            return this;
        }

        public Iotics build() {
            if (configuration == null) {
                throw new IllegalArgumentException("null configuration");
            }
            if (Strings.isNullOrEmpty(configuration.hostDNS())) {
                throw new IllegalArgumentException("invalid hostDNS: null or empty");
            }

            if (registry == null) {
                registry = new HttpServiceRegistry(configuration.hostDNS());
            }
            if (endpoints == null) {
                try {
                    endpoints = registry.find();
                } catch (IOException ioe) {
                    throw new IllegalArgumentException("invalid configuration: can't access host endpoints via hostDNS:" + configuration.hostDNS(), ioe);
                }
            }

            if (sim == null) {
                try {
                    sim = Tools.newSimpleIdentityManager(configuration, endpoints.resolver());
                } catch (FileNotFoundException e) {
                    throw new IllegalArgumentException("unable to load library", e);
                }
            }
            if (api == null) {
                try {
                    api = Tools.newIoticsApi(sim, endpoints.grpc(), Duration.ofSeconds(configuration.tokenDuration()));
                } catch (IOException e) {
                    throw new IllegalArgumentException("unable to instantiate api", e);
                }
            }

            return new Iotics(sim, api, configuration, registry, endpoints);
        }
    }

}
