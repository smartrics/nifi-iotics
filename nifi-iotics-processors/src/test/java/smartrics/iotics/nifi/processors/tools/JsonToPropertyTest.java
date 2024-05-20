package smartrics.iotics.nifi.processors.tools;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

import static smartrics.iotics.nifi.processors.tools.JsonToProperty.*;

import com.google.gson.JsonObject;
import com.iotics.api.Property;
import org.junit.jupiter.api.Test;


class JsonToPropertyTest {
    @Test
    public void testFromJsonMissingKey() {
        JsonObject o = new JsonObject();
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> fromJson(o));
        assertThat(exception.getMessage(), is("Invalid property: missing \"key\""));
    }

    @Test
    public void testFromJsonMissingLiteralDataType() {
        JsonObject o = new JsonObject();
        o.addProperty("key", "someKey");
        o.addProperty("literal", "someLiteral");
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> fromJson(o));
        assertThat(exception.getMessage(), is("Invalid property: literal without \"dataType\""));
    }

    @Test
    public void testFromJsonMissingLangLiteralLang() {
        JsonObject o = new JsonObject();
        o.addProperty("key", "someKey");
        o.addProperty("langLiteral", "someLangLiteral");
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> fromJson(o));
        assertThat(exception.getMessage(), is("Invalid property: langLiteral without \"lang\""));
    }

    @Test
    public void testFromJsonMissingAllOptionalFields() {
        JsonObject o = new JsonObject();
        o.addProperty("key", "someKey");
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> fromJson(o));
        assertThat(exception.getMessage(), is("Invalid property: missing one of \"uri\", \"literal\", \"stringLiteral\", \"langLiteral\" "));
    }

    @Test
    public void testFromJsonWithLiteral() {
        JsonObject o = new JsonObject();
        o.addProperty("key", "someKey");
        o.addProperty("literal", "someLiteral");
        o.addProperty("dataType", "someDataType");

        Property property = fromJson(o);

        assertThat(property.getKey(), is("someKey"));
        assertThat(property.getLiteralValue().getValue(), is("someLiteral"));
        assertThat(property.getLiteralValue().getDataType(), is("someDataType"));
    }

    @Test
    public void testFromJsonWithLangLiteral() {
        JsonObject o = new JsonObject();
        o.addProperty("key", "someKey");
        o.addProperty("langLiteral", "someLangLiteral");
        o.addProperty("lang", "en");

        Property property = fromJson(o);

        assertThat(property.getKey(), is("someKey"));
        assertThat(property.getLangLiteralValue().getValue(), is("someLangLiteral"));
        assertThat(property.getLangLiteralValue().getLang(), is("en"));
    }

    @Test
    public void testFromJsonWithStringLiteral() {
        JsonObject o = new JsonObject();
        o.addProperty("key", "someKey");
        o.addProperty("stringLiteral", "someStringLiteral");

        Property property = fromJson(o);

        assertThat(property.getKey(), is("someKey"));
        assertThat(property.getStringLiteralValue().getValue(), is("someStringLiteral"));
    }

    @Test
    public void testFromJsonWithUri() {
        JsonObject o = new JsonObject();
        o.addProperty("key", "someKey");
        o.addProperty("uri", "http://example.com");

        Property property = fromJson(o);

        assertThat(property.getKey(), is("someKey"));
        assertThat(property.getUriValue().getValue(), is("http://example.com"));
    }

}