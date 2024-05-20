package smartrics.iotics.nifi.processors;

import com.bazaarvoice.jolt.Chainr;
import com.bazaarvoice.jolt.JsonUtils;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.core.RDFDataset;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class JoltTest {

    private static String inputJson;

    private static String specJson;

    @BeforeAll
    public static void setup() throws IOException {
        inputJson = Files.readString(Path.of("src/test/resources/car.json"));
        specJson = Files.readString(Path.of("src/test/resources/car_twin_to_jsonld_jolt_spec.json"));
    }

    @Test
    public void testCarTransformation() {
        // Input JSON
        // Convert Strings to JSON Objects
        Object input = JsonUtils.jsonToObject(inputJson);
        List<Object> spec = JsonUtils.jsonToList(specJson);

        // Perform the Jolt transformation
        Chainr chainr = Chainr.fromSpec(spec);
        Object actualOutput = chainr.transform(input);

        JsonLdOptions options = new JsonLdOptions();
        // Convert JSON-LD to RDF triples
        RDFDataset dataset = (RDFDataset) JsonLdProcessor.toRDF(actualOutput, options);
        String defaultGraph = dataset.keySet().iterator().next();
        List<RDFDataset.Quad> quads = dataset.getQuads(defaultGraph);

        RDFDataset.Quad typeQuad = quadFor("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", quads);
        assertThat(typeQuad.getObject().getValue(), is(equalTo("http://schema.org/Car")));

        RDFDataset.Quad labelQuad = quadFor("http://www.w3.org/2000/01/rdf-schema#label", quads);
        assertThat(labelQuad.getObject().getValue(), is(equalTo("Toyota Camry 1")));
        assertThat(labelQuad.getObject().getDatatype(), is(equalTo("http://www.w3.org/2001/XMLSchema#string")));
    }

    private RDFDataset.Quad quadFor(String s, List<RDFDataset.Quad> quads) {
        return quads.stream().filter(quad ->
                quad.getPredicate().getValue().equals(s)).findFirst().orElseThrow();
    }
}
