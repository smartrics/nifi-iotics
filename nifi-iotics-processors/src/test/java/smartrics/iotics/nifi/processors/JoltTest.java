package smartrics.iotics.nifi.processors;

import com.bazaarvoice.jolt.Chainr;
import com.bazaarvoice.jolt.JsonUtils;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.core.RDFDataset;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import smartrics.iotics.nifi.processors.objects.MyProperty;
import smartrics.iotics.nifi.processors.objects.MyTwinModel;
import smartrics.iotics.nifi.processors.objects.Port;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class JoltTest {

    @BeforeAll
    public static void setup() {
    }

    private static Object convert(String inputJson, String specJson) {
        Object input = JsonUtils.jsonToObject(inputJson);
        List<Object> spec = JsonUtils.jsonToList(specJson);

        // Perform the Jolt transformation
        Chainr chainr = Chainr.fromSpec(spec);
        return chainr.transform(input);
    }

    @Test
    public void testCarToJson() throws IOException {
        String inputJson = Files.readString(Path.of("src/test/resources/car.json"));
        String specJson = Files.readString(Path.of("src/test/resources/car_twin_to_json_jolt_spec.json"));
        Object actualOutput = convert(inputJson, specJson);
        Gson g = new GsonBuilder().setPrettyPrinting().create();
        String s = g.toJson(actualOutput);
        MyTwinModel m = g.fromJson(s, MyTwinModel.class);
        List<MyProperty> properties = m.properties();

        properties.forEach(p -> System.out.println(g.toJson(p)));

        MyProperty prop = properties.stream().filter(p -> p.key().equals("http://schema.org/identifier")).findFirst().orElseThrow();
        assertThat(prop.value(), is("1"));
        assertThat(prop.type(), is("StringLiteral"));

        prop = properties.stream().filter(p -> p.key().equals("http://schema.org/manufacturer")).findFirst().orElseThrow();
        assertThat(prop.value(), is("Toyota"));
        assertThat(prop.type(), is("StringLiteral"));

        prop = properties.stream().filter(p -> p.key().equals("http://schema.org/model")).findFirst().orElseThrow();
        assertThat(prop.value(), is("Camry"));
        assertThat(prop.type(), is("StringLiteral"));

        prop = properties.stream().filter(p -> p.key().equals("http://schema.org/color")).findFirst().orElseThrow();
        assertThat(prop.value(), is("Red"));
        assertThat(prop.type(), is("StringLiteral"));

        prop = properties.stream().filter(p -> p.key().equals("http://schema.org/givenName")).findFirst().orElseThrow();
        assertThat(prop.value(), is("Bob"));
        assertThat(prop.type(), is("StringLiteral"));

        prop = properties.stream().filter(p -> p.key().equals("http://data.iotics.com/nifi/isOperational")).findFirst().orElseThrow();
        assertThat(prop.value(), is("true"));
        assertThat(prop.type(), is("StringLiteral"));

        prop = properties.stream().filter(p -> p.key().equals("http://www.w3.org/2000/01/rdf-schema#comment")).findFirst().orElseThrow();
        assertThat(prop.value(), is("Well maintained"));
        assertThat(prop.type(), is("StringLiteral"));

        prop = properties.stream().filter(p -> p.key().equals("https://data.iotics.com/nifi/units")).findFirst().orElseThrow();
        assertThat(prop.value(), is("5"));
        assertThat(prop.type(), is("StringLiteral"));

        prop = properties.stream().filter(p -> p.key().equals("http://data.iotics.com/public#hostAllowList")).findFirst().orElseThrow();
        assertThat(prop.value(), is("http://data.iotics.com/public#none"));
        assertThat(prop.type(), is("Uri"));

        prop = properties.stream().filter(p -> p.key().equals("http://data.iotics.com/public#hostMetadataAllowList")).findFirst().orElseThrow();
        assertThat(prop.value(), is("http://data.iotics.com/public#none"));
        assertThat(prop.type(), is("Uri"));

        prop = properties.stream().filter(p -> p.key().equals("http://www.w3.org/2000/01/rdf-schema#label")).findFirst().orElseThrow();
        assertThat(prop.value(), is("Toyota Camry 1"));
        assertThat(prop.type(), is("StringLiteral"));

        List<Port> inputs = m.inputs();
        assertThat(inputs, is(empty()));

        Port feed = m.feeds().stream().findFirst().orElseThrow();
        assertThat(feed.id(), is("status"));
    }

    @Test
    public void testCarToJsonLD() throws IOException {
        String inputJson = Files.readString(Path.of("src/test/resources/car.json"));
        String specJson = Files.readString(Path.of("src/test/resources/car_twin_to_jsonld_jolt_spec.json"));
        Object actualOutput = convert(inputJson, specJson);

        JsonLdOptions options = new JsonLdOptions();
        // Convert JSON-LD to RDF triples
        RDFDataset dataset = (RDFDataset) JsonLdProcessor.toRDF(actualOutput, options);
        String defaultGraph = dataset.keySet().iterator().next();
        List<RDFDataset.Quad> quads = dataset.getQuads(defaultGraph);

        RDFDataset.Quad keyQuad = quadFor("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", quads);
        assertThat(keyQuad.getObject().getValue(), is(equalTo("http://schema.org/Car")));

        RDFDataset.Quad labelQuad = quadFor("http://www.w3.org/2000/01/rdf-schema#label", quads);
        assertThat(labelQuad.getObject().getValue(), is(equalTo("Toyota Camry 1")));
        assertThat(labelQuad.getObject().getDatatype(), is(equalTo("http://www.w3.org/2001/XMLSchema#string")));
    }

    private RDFDataset.Quad quadFor(String s, List<RDFDataset.Quad> quads) {
        return quads.stream().filter(quad ->
                quad.getPredicate().getValue().equals(s)).findFirst().orElseThrow();
    }
}
