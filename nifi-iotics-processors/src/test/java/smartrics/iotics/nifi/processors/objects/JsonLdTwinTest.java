package smartrics.iotics.nifi.processors.objects;

import com.github.jsonldjava.core.RDFDataset;
import com.iotics.api.LangLiteral;
import com.iotics.api.Literal;
import com.iotics.api.Property;
import com.iotics.api.Uri;
import org.apache.nifi.logging.ComponentLog;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import smartrics.iotics.connectors.twins.annotations.XsdDatatype;
import smartrics.iotics.host.IoticsApi;
import smartrics.iotics.host.UriConstants;
import smartrics.iotics.identity.Identity;
import smartrics.iotics.identity.SimpleIdentityManager;

import java.net.URI;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

class JsonLdTwinTest {

    @Mock
    public IoticsApi api;

    @Mock
    public SimpleIdentityManager sim;

    @Mock
    public ComponentLog logger;

    private Identity myId;

    private JsonLdTwin twin;
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
        twin = newTwin(List.of());
        assertThat(twin.getUpsertTwinRequest().getHeaders().getClientAppId(), is(equalTo(agentId.did())));
    }

    @Test
    void addsLangPropertyWithStringType() {
        List<RDFDataset.Quad> quads = List.of(q("http://schema.org/foo", "1", XsdDatatype.byte_, "en"));
        twin = newTwin(quads);
        LangLiteral literal = findLang("http://schema.org/foo", "en").orElseThrow().getLangLiteralValue();
        assertThat(literal.getValue(), is(equalTo("1")));
        assertThat(literal.getLang(), is(equalTo("en")));
    }

    @Test
    void addsStringProperty() {
        List<RDFDataset.Quad> quads = List.of(q("http://schema.org/foo", "bar", "string"));
        twin = newTwin(quads);
        Literal literal = find("http://schema.org/foo", "string").orElseThrow().getLiteralValue();
        assertThat(literal.getValue(), is(equalTo("bar")));
        assertThat(literal.getDataType(), is(equalTo("string")));
    }

    @Test
    void mapsXSDDatatypesToTheirName() {
        List<RDFDataset.Quad> quads = List.of(q("http://schema.org/foo", "1", "http://www.w3.org/2001/XMLSchema#integer"));
        twin = newTwin(quads);
        Literal literal = find("http://schema.org/foo", "integer").orElseThrow().getLiteralValue();
        assertThat(literal.getValue(), is(equalTo("1")));
        assertThat(literal.getDataType(), is(equalTo("integer")));
    }

    @Test
    void mapsIRIToUri() {
        List<RDFDataset.Quad> quads = List.of(q("http://schema.org/foo", new RDFDataset.IRI("http://schema.org/Car")));
        twin = newTwin(quads);
        Uri uri = findURI("http://schema.org/foo").orElseThrow().getUriValue();
        assertThat(uri.getValue(), is(equalTo("http://schema.org/Car")));
    }

    @Test
    void addsLiteralProperty() {
        List<RDFDataset.Quad> quads = List.of(q("http://schema.org/foo", "1", XsdDatatype.integer));
        twin = newTwin(quads);
        Literal literal = find("http://schema.org/foo", "integer").orElseThrow().getLiteralValue();
        assertThat(literal.getValue(), is(equalTo("1")));
        assertThat(literal.getDataType(), is(equalTo("integer")));
    }

    @Test
    void addAllowListPropFromDefaultIfNotPresent() {
        List<RDFDataset.Quad> quads = List.of();
        String defAllowListValue = "did:iotics:1,did:iotics:2";
        twin = new JsonLdTwin(logger, api, sim, quads, myId, defAllowListValue);
        Uri uri = findURI(UriConstants.IOTICSProperties.HostAllowListName).orElseThrow().getUriValue();
        assertThat(uri.getValue(), is(equalTo(defAllowListValue)));
    }

    @Test
    void addAllowListPropFromQuadsIfPresent() {
        List<RDFDataset.Quad> quads = List.of(q(UriConstants.IOTICSProperties.HostAllowListName, UriConstants.IOTICSProperties.HostAllowListValues.ALL.toString()));
        String defAllowListValue = "did:iotics:1,did:iotics:2";
        twin = new JsonLdTwin(logger, api, sim, quads, myId, defAllowListValue);
        Uri uri = findURI(UriConstants.IOTICSProperties.HostAllowListName).orElseThrow().getUriValue();
        assertThat(uri.getValue(), is(equalTo(UriConstants.IOTICSProperties.HostAllowListValues.ALL.toString())));
    }

    @Test
    void throwsIfDefAllowListInvalid() {
        IllegalArgumentException t = assertThrows(IllegalArgumentException.class, new Executable() {
            @Override
            public void execute() {
                new JsonLdTwin(logger, api, sim, List.of(), myId, "wrong:thing");
            }
        });
        assertThat(t.getMessage(), is(equalTo("invalid default for allow list value: wrong:thing")));
    }

    private @NotNull JsonLdTwin newTwin(List<RDFDataset.Quad> quads) {
        return new JsonLdTwin(logger, api, sim, quads, myId, UriConstants.IOTICSProperties.HostAllowListValues.NONE.toString());
    }

    public Optional<Property> find(String key, String type) {
        return twin.getUpsertTwinRequest().getPayload().getPropertiesList().stream().filter(p ->
                        p.getKey().equals(key) &&
                                p.hasLiteralValue() &&
                                p.getLiteralValue().getDataType().equals(type))
                .findFirst();
    }

    public Optional<Property> findURI(String key) {
        return twin.getUpsertTwinRequest().getPayload().getPropertiesList().stream().filter(p ->
                        p.getKey().equals(key) &&
                                p.hasUriValue())
                .findFirst();
    }

    public Optional<Property> findLang(String key, String lang) {
        return twin.getUpsertTwinRequest().getPayload().getPropertiesList().stream().filter(p ->
                        p.getKey().equals(key) &&
                                p.hasLangLiteralValue() &&
                                p.getLangLiteralValue().getLang().equals(lang))
                .findFirst();
    }

    public RDFDataset.Quad q(String prop, RDFDataset.IRI iri) {
        return new RDFDataset.Quad(new RDFDataset.IRI(myId.did()), new RDFDataset.IRI(prop), iri, null);
    }

    public RDFDataset.Quad q(String prop, String value, XsdDatatype type) {
        return new RDFDataset.Quad(myId.did(), URI.create(prop).toString(), value, type.toString(), null, null);
    }

    public RDFDataset.Quad q(String prop, String value, String type) {
        return new RDFDataset.Quad(myId.did(), URI.create(prop).toString(), value, type, null, null);
    }

    public RDFDataset.Quad q(String prop, String value, XsdDatatype type, String lang) {
        return new RDFDataset.Quad(myId.did(), URI.create(prop).toString(), value, type.toString(), lang, null);
    }

    public RDFDataset.Quad q(String prop, String value) {
        return new RDFDataset.Quad(myId.did(), URI.create(prop).toString(), value, null, null, null);
    }
}