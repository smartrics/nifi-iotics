package smartrics.iotics.nifi.processors.objects;

import com.iotics.api.UpsertFeedWithMeta;
import com.iotics.api.UpsertInputWithMeta;
import org.checkerframework.checker.units.qual.A;
import org.junit.jupiter.api.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PortTest {

    @Test
    void testPortConstructorWithValidInputs() {
        List<MyProperty> properties = List.of(new MyProperty("key1", "value1", null, null, null));
        List<MyValue> values = List.of(new MyValue("value1", "dataType", "comment", "value"));
        Map<String, Object> payload = Map.of("key", "value");
        Port port = new Port("portId", properties, values, true);

        assertThat(port.id(), is("portId"));
        assertThat(port.properties(), is(properties));
        assertThat(port.values(), is(values));
        assertThat(port.storeLast(), is(true));
    }

    @Test
    void testPortConstructorWithNullValues() {
        Port port = new Port("portId", null, null, true);

        assertThat(port.id(), is("portId"));
        assertThat(port.properties(), is(empty()));
        assertThat(port.values(), is(empty()));
        assertThat(port.storeLast(), is(true));
    }

    @Test
    void testFeedFactory() {
        List<MyProperty> properties = List.of(new MyProperty("key1", "value1", "StringLiteral", null, null));
        List<MyValue> values = List.of(new MyValue("value1", "dataType", "comment"));
        Port port = new Port("portId", properties, values, true);

        UpsertFeedWithMeta result = Port.feedFactory(port);

        assertThat(result.getId(), is("portId"));
        assertThat(result.getStoreLast(), is(true));
        // You would need to further assert the properties and values are correctly added
    }

    @Test
    void testInputsFactory() {
        List<MyProperty> properties = List.of(new MyProperty("key1", "value1", "StringLiteral", null, null));
        List<MyValue> values = List.of(new MyValue("value1", "dataType", "comment"));
        Port port = new Port("portId", properties, values, false);

        UpsertInputWithMeta result = Port.inputsFactory(port);

        assertThat(result.getId(), is("portId"));
        // You would need to further assert the properties and values are correctly added
    }

    @Test
    void parseFromJson() {
        Port p = Port.fromJson("""
                {
                   "id": "feed1"
                }
            """);
        assertNotNull(p.values());
        assertEquals("feed1", p.id());

    }
}