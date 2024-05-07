package smartrics.iotics.nifi.processors.objects;

import com.google.gson.JsonObject;
import com.iotics.api.Property;
import com.iotics.api.UpsertTwinRequest;
import org.apache.nifi.logging.ComponentLog;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import smartrics.iotics.host.IoticsApi;
import smartrics.iotics.host.UriConstants;
import smartrics.iotics.identity.Identity;
import smartrics.iotics.identity.SimpleIdentityManager;

import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

public class ConcreteTwinTest {

    public static final String ONT_PREFIX = "http://hello.com/ont#";
    private ComponentLog loggerMock;
    private IoticsApi apiMock;
    private SimpleIdentityManager simMock;

    private final Identity agentIdentity = new Identity("aKey", "aName", "did:iotics:234");
    private final Identity myIdentity = new Identity("tKey", "tName", "did:iotics:432");

    @BeforeEach
    void setUp() {
        loggerMock = Mockito.mock(ComponentLog.class);
        apiMock = Mockito.mock(IoticsApi.class);
        simMock = Mockito.mock(SimpleIdentityManager.class);
        when(simMock.agentIdentity()).thenReturn(agentIdentity);
    }

    @Test
    void setsHeadersCorrectly() {
        JsonObject source = new JsonObject();
        ConcreteTwin concreteTwin = aConcreteTwin(source);
        UpsertTwinRequest request = concreteTwin.getUpsertTwinRequest();
        assertThat(request.getHeaders().getClientAppId(), is(equalTo("did:iotics:234")));
    }

    @Test
    void setsTwinIdCorrectly() {
        JsonObject source = new JsonObject();
        ConcreteTwin concreteTwin = aConcreteTwin(source);
        UpsertTwinRequest request = concreteTwin.getUpsertTwinRequest();
        assertThat(request.getPayload().getTwinId().getId(), is(equalTo("did:iotics:432")));
    }

    @Test
    void setsVisibility() {
        JsonObject source = new JsonObject();
        ConcreteTwin concreteTwin = aConcreteTwin(source);
        UpsertTwinRequest request = concreteTwin.getUpsertTwinRequest();
        assertThat(getPropByKey(request, UriConstants.IOTICSProperties.HostAllowListName).getUriValue().getValue(),
                is(equalTo(UriConstants.IOTICSProperties.HostAllowListValues.ALL.toString())));
    }

    @Test
    void ignoresComplexObjects() {
        JsonObject source = new JsonObject();
        source.add("complex", new JsonObject());
        ConcreteTwin concreteTwin = aConcreteTwin(source);
        UpsertTwinRequest request = concreteTwin.getUpsertTwinRequest();
        assertThat(hasPropByKey(request, "complex"), is(equalTo(false)));
    }

    @Test
    void usesPrefixIfPropertiesNotStartingWithSchema() {
        JsonObject source = new JsonObject();
        source.addProperty("testKeyString", "testValue");

        UpsertTwinRequest request = aConcreteTwin(source).getUpsertTwinRequest();

        System.out.println(request);
        assertThat(getPropByKey(request, ONT_PREFIX + "testKeyString").getStringLiteralValue().getValue(),
                is(equalTo("testValue")));
    }

    @Test
    void correctUpsertTwinRequestWithJsonPrimitiveProperties() {
        JsonObject source = new JsonObject();
        source.addProperty("http://testKeyBoolean", true);
        source.addProperty("http://testKeyNumber", 123);
        source.addProperty("http://testKeyString", "testValue");
        source.addProperty("http://testKeyUri", "http://value.com");
        source.addProperty("http://testKeyDate", "2024-02-12");
        source.addProperty("http://testKeyTime", "15:30:00");
        source.addProperty("http://testKeyDateTime", "2024-05-07T15:24:00");

        UpsertTwinRequest request = aConcreteTwin(source).getUpsertTwinRequest();

        assertThat(getPropByKey(request, "http://testKeyBoolean").getLiteralValue().getValue(),
                is(equalTo("true")));
        assertThat(getPropByKey(request, "http://testKeyBoolean").getLiteralValue().getDataType(),
                is(equalTo("boolean")));

        assertThat(getPropByKey(request, "http://testKeyNumber").getLiteralValue().getValue(),
                is(equalTo("123")));
        assertThat(getPropByKey(request, "http://testKeyNumber").getLiteralValue().getDataType(),
                is(equalTo("decimal")));

        assertThat(getPropByKey(request, "http://testKeyDateTime").getLiteralValue().getValue(),
                is(equalTo("2024-05-07T15:24:00")));
        assertThat(getPropByKey(request, "http://testKeyDateTime").getLiteralValue().getDataType(),
                is(equalTo("dateTime")));

        assertThat(getPropByKey(request, "http://testKeyTime").getLiteralValue().getValue(),
                is(equalTo("15:30:00")));
        assertThat(getPropByKey(request, "http://testKeyTime").getLiteralValue().getDataType(),
                is(equalTo("time")));

        assertThat(getPropByKey(request, "http://testKeyDate").getLiteralValue().getValue(),
                is(equalTo("2024-02-12")));
        assertThat(getPropByKey(request, "http://testKeyDate").getLiteralValue().getDataType(),
                is(equalTo("date")));

        assertThat(getPropByKey(request, "http://testKeyUri").getUriValue().getValue(),
                is(equalTo("http://value.com")));

        assertThat(getPropByKey(request, "http://testKeyString").getStringLiteralValue().getValue(),
                is(equalTo("testValue")));

    }

    private @NotNull ConcreteTwin aConcreteTwin(JsonObject source) {
        return new ConcreteTwin(loggerMock, apiMock, simMock, source, ONT_PREFIX, myIdentity);
    }

    private static @NotNull Property getPropByKey(UpsertTwinRequest request, String keyToFind) {
        Stream<Property> stream = request.getPayload().getPropertiesList().stream();
        return stream.filter(property -> property.getKey().equals(keyToFind)).findFirst().orElseThrow();
    }

    private static boolean hasPropByKey(UpsertTwinRequest request, String keyToFind) {
        Stream<Property> stream = request.getPayload().getPropertiesList().stream();
        return stream.anyMatch(property -> property.getKey().equals(keyToFind));
    }


}
