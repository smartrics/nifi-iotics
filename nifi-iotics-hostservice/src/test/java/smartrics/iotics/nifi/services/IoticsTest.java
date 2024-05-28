package smartrics.iotics.nifi.services;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import smartrics.iotics.host.HostEndpoints;
import smartrics.iotics.host.HttpServiceRegistry;
import smartrics.iotics.host.IoticsApi;
import smartrics.iotics.identity.SimpleIdentityManager;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class IoticsTest {

    private SimpleIdentityManager sim;
    private HttpServiceRegistry registry;
    private Configuration configuration;
    private HostEndpoints endpoints;
    private IoticsApi api;
    private IoticsFactory iFactory;

    private static @NotNull Configuration newConfiguration() {
        return new Configuration("seed", "userKey", "agentKey", "fooHostDNS", 3600, 16, "../lib");
    }

    private @NotNull Iotics newIotics() {
        return Iotics.Builder.newBuilder()
                .withSim(sim)
                .withIoticsFactory(iFactory)
                .withApi(api)
                .withConfiguration(configuration)
                .withRegistry(registry)
                .withEndpoints(endpoints)
                .build();
    }

    @BeforeEach
    void setup() {
        configuration = newConfiguration();
        endpoints = Mockito.mock(HostEndpoints.class);
        iFactory = Mockito.mock(IoticsFactory.class);
        registry = Mockito.mock(HttpServiceRegistry.class);
        sim = Mockito.mock(SimpleIdentityManager.class);
        api = Mockito.mock(IoticsApi.class);
    }


    @Test
    public void testBuilderWithValidConfiguration() {
        Iotics iotics = newIotics();
        assertThat(iotics.sim(), is(sim));
        assertThat(iotics.api(), is(api));
        assertThat(iotics.configuration(), is(configuration));
        assertThat(iotics.registry(), is(registry));
        assertThat(iotics.endpoints(), is(endpoints));
    }


    @Test
    public void testBuilderWithNullConfiguration() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> {
            Iotics.Builder.newBuilder().build();
        });
        assertThat(thrown.getMessage(), is("null configuration"));
    }

    @Test
    public void testBuilderWithInvalidHostDNS() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> {
            Iotics.Builder.newBuilder().withConfiguration(configuration).build();
        });
        assertThat(thrown.getMessage(), is("invalid configuration: can't access host endpoints via hostDNS:fooHostDNS"));
    }

    @Test
    public void testBuilderWithNullRegistry() {
        Mockito.when(endpoints.resolver()).thenReturn("http://resolver.com");

        Iotics iotics = newIotics();
        assertThat(iotics.configuration(), is(configuration));
        assertThat(iotics.registry(), is(notNullValue()));
        assertThat(iotics.endpoints(), is(endpoints));
    }

    @Test
    public void testBuilderWithNullSim() throws IOException {
        Mockito.when(endpoints.resolver()).thenReturn("http://resolver.com");
        Mockito.when(registry.find()).thenReturn(endpoints);
        SimpleIdentityManager builtSim = Mockito.mock(SimpleIdentityManager.class);
        Mockito.when(iFactory.newSimpleIdentityManager(configuration, "http://resolver.com")).thenReturn(builtSim);

        Iotics iotics = Iotics.Builder.newBuilder()
                .withSim(null)
                .withIoticsFactory(iFactory)
                .withApi(api)
                .withConfiguration(configuration)
                .withRegistry(registry)
                .withEndpoints(endpoints)
                .build();

        assertThat(iotics.configuration(), is(configuration));
        assertThat(iotics.registry(), is(registry));
        assertThat(iotics.sim(), is(builtSim));
    }

    @Test
    public void testBuilderWithNullApi() throws IOException {
        Mockito.when(endpoints.resolver()).thenReturn("http://resolver.com");
        Mockito.when(endpoints.grpc()).thenReturn("grpc.iotics.com");
        Mockito.when(registry.find()).thenReturn(endpoints);

        Iotics iotics = newIotics();

        assertThat(iotics.configuration(), is(configuration));
        assertThat(iotics.registry(), is(registry));
        assertThat(iotics.sim(), is(sim));
        assertThat(iotics.api(), is(api));
    }

}
