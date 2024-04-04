package smartrics.iotics.nifi.processors;

import com.bazaarvoice.jolt.Chainr;
import com.bazaarvoice.jolt.JsonUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class JoltTest {


    private static String inputJson;

    private static String specJson;

    @BeforeAll
    public static void setup() throws IOException {
        inputJson = Files.readString(Path.of("src/test/resources/input.json"));
        specJson = Files.readString(Path.of("src/test/resources/joltSpec.json"));
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

        // Assert the transformation output matches expected output
        System.out.println(JsonUtils.toJsonString(actualOutput));
    }
}
