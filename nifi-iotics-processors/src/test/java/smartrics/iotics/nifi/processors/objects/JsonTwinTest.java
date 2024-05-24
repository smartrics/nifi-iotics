package smartrics.iotics.nifi.processors.objects;

import com.iotics.api.LangLiteral;
import com.iotics.api.Literal;
import com.iotics.api.Uri;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import smartrics.iotics.host.IoticsApi;
import smartrics.iotics.identity.Identity;
import smartrics.iotics.identity.SimpleIdentityManager;

import java.util.HashMap;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;
import static smartrics.iotics.nifi.processors.objects.Utils.*;

class JsonTwinTest {

    @Mock
    public IoticsApi api;

    @Mock
    public SimpleIdentityManager sim;

    private Identity myId;

    private JsonTwin twin;
    private Identity agentId;

    @BeforeEach
    public void init() {
        MockitoAnnotations.openMocks(this);
        myId = new Identity("myKey", "myName", "did:iotics:1234");
        agentId = new Identity("agKey", "agName", "did:iotics:4321");
        when(sim.agentIdentity()).thenReturn(agentId);
    }

    @Test
    void addsHeaders() {
        twin = newTwin(new MyTwinModel("host", "id", List.of(), List.of(), List.of()));
        assertThat(twin.getUpsertTwinRequest().getHeaders().getClientAppId(), is(equalTo(agentId.did())));
    }


    @Test
    void addsLangPropertyWithStringType() {
        List<MyProperty> props = List.of(new MyProperty("http://schema.org/foo", LangLiteral.newBuilder().setLang("en").setValue("1").build()));
        twin = newTwin(MyTwinModel.Builder.aMyTwinModel().withProperties(props).withId("id").withHostId("hid").build());
        LangLiteral literal = findLang(twin, "http://schema.org/foo", "en").orElseThrow().getLangLiteralValue();
        assertThat(literal.getValue(), is(equalTo("1")));
        assertThat(literal.getLang(), is(equalTo("en")));
    }

    @Test
    void addsStringProperty() {
        List<MyProperty> props = List.of(new MyProperty("http://schema.org/foo", Literal.newBuilder().setDataType("string").setValue("bar").build()));
        twin = newTwin(MyTwinModel.Builder.aMyTwinModel().withId("id").withHostId("hid").withProperties(props).build());
        Literal literal = find(twin, "http://schema.org/foo", "string").orElseThrow().getLiteralValue();
        assertThat(literal.getValue(), is(equalTo("bar")));
        assertThat(literal.getDataType(), is(equalTo("string")));
    }

    @Test
    void addUriProperty() {
        List<MyProperty> props = List.of(new MyProperty("http://schema.org/foo", Uri.newBuilder().setValue("http://schema.org/Car").build()));
        twin = newTwin(MyTwinModel.Builder.aMyTwinModel().withId("id").withHostId("hid").withProperties(props).build());
        Uri uri = findURI(twin, "http://schema.org/foo").orElseThrow().getUriValue();
        assertThat(uri.getValue(), is(equalTo("http://schema.org/Car")));
    }

    @Test
    void generatesShareRequest() {
        MyValue myValue = new MyValue("l", "dt", "c");
        Port myFeed = new Port("feedId", List.of(), List.of(myValue), false);
        MyTwinModel model = MyTwinModel.Builder.aMyTwinModel().withId("id").withHostId("hid")
                .withFeeds(List.of(myFeed))
                .build();
        twin = newTwin(model);
    }

    private @NotNull JsonTwin newTwin(MyTwinModel model) {
        return new JsonTwin(api, sim, myId, model);
    }

}