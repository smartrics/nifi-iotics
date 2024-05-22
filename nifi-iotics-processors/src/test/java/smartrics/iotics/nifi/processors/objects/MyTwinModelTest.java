package smartrics.iotics.nifi.processors.objects;

import com.google.gson.Gson;
import com.iotics.api.SearchResponse;
import com.iotics.api.TwinID;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class MyTwinModelTest {

    @Test
    void testConstructorWithAllParameters() {
        List<MyProperty> properties = List.of(new MyProperty("key1", "value1", "", "", ""));
        List<Port> feeds = List.of(new Port("feed1", List.of(), List.of(), true));
        List<Port> inputs = List.of(new Port("input1", List.of(), List.of(), true));

        MyTwinModel myTwinModel = new MyTwinModel("hostDid1", "id1", properties, feeds, inputs);

        assertEquals("hostDid1", myTwinModel.hostDid());
        assertEquals("id1", myTwinModel.id());
        assertEquals(properties, myTwinModel.properties());
        assertEquals(feeds, myTwinModel.feeds());
        assertEquals(inputs, myTwinModel.inputs());
    }

    @Test
    void testConstructorWithTwinDetails() {
        SearchResponse.TwinDetails twinDetails = SearchResponse.TwinDetails.newBuilder()
                .setTwinId(TwinID.newBuilder().setId("id").build())
                .setTwinId(TwinID.newBuilder().setHostId("hid").build())
                .build();
        MyTwinModel myTwinModel = new MyTwinModel(twinDetails);

        assertEquals(twinDetails.getTwinId().getHostId(), myTwinModel.hostDid());
        assertEquals(twinDetails.getTwinId().getId(), myTwinModel.id());
    }

    @Test
    void testFindProperty() {
        MyProperty property = new MyProperty("key2", "value2", "", "", "");
        List<MyProperty> properties = List.of(property);
        MyTwinModel myTwinModel = new MyTwinModel("hostDid2", "id2", properties, List.of(), List.of());

        Optional<MyProperty> foundProperty = myTwinModel.findProperty("key2");

        assertTrue(foundProperty.isPresent());
        assertEquals(property, foundProperty.get());
    }

    @Test
    void testFindPropertyNotFound() {
        List<MyProperty> properties = List.of(new MyProperty("key3", "value3", "", "", ""));
        MyTwinModel myTwinModel = new MyTwinModel("hostDid3", "id3", properties, List.of(), List.of());

        Optional<MyProperty> foundProperty = myTwinModel.findProperty("key4");

        assertFalse(foundProperty.isPresent());
    }

    @Test
    void testToJson() {
        List<MyProperty> properties = List.of(new MyProperty("key5", "value5", "", "", ""));
        List<Port> feeds = List.of(new Port("feed5", null, null, true));
        List<Port> inputs = List.of(new Port("input5", null, null, true));
        MyTwinModel myTwinModel = new MyTwinModel("hostDid5", "id5", properties, feeds, inputs);

        String json = myTwinModel.toJson();
        Gson gson = new Gson();
        MyTwinModel deserialized = gson.fromJson(json, MyTwinModel.class);

        assertEquals(myTwinModel, deserialized);
    }

    @Test
    void testBuilder() {
        List<MyProperty> properties = List.of(new MyProperty("key6", "value6", "", "", ""));
        List<Port> feeds = List.of(new Port("feed6", null, null, true));
        List<Port> inputs = List.of(new Port("input6", null, null, true));

        MyTwinModel myTwinModel = MyTwinModel.Builder.aMyTwinModel()
                .withHostDid("hostDid6")
                .withId("id6")
                .withProperties(properties)
                .withFeeds(feeds)
                .withInputs(inputs)
                .build();

        assertEquals("hostDid6", myTwinModel.hostDid());
        assertEquals("id6", myTwinModel.id());
        assertEquals(properties, myTwinModel.properties());
        assertEquals(feeds, myTwinModel.feeds());
        assertEquals(inputs, myTwinModel.inputs());
    }

    @Test
    void parseFromJsonShouldCreateValidObject() {
        String content = """
                {
                    "hostId": "12345678",
                    "id": "0987654",
                    "feeds": []
                }
                """;
        MyTwinModel myTwin = MyTwinModel.fromJson(content);
        assertNotNull(myTwin.properties());
        assertNotNull(myTwin.feeds());
        assertNotNull(myTwin.inputs());

    }
}
