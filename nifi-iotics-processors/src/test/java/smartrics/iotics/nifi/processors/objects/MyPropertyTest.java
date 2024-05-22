package smartrics.iotics.nifi.processors.objects;

import com.iotics.api.*;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static smartrics.iotics.nifi.processors.objects.MyProperty.factory;

class MyPropertyTest {

    @Test
    void factoryOfFeeds() {
    }

    @Test
    void factoryOfInputs() {
    }


    @Test
    void testFactoryWithUri() {
        MyProperty prop = new MyProperty("key1", "http://example.com", "Uri", null, null);
        Property result = factory(prop);

        assertThat(result.getKey(), is("key1"));
        assertThat(result.getUriValue().getValue(), is("http://example.com"));
    }

    @Test
    void testFactoryWithStringLiteral() {
        MyProperty prop = new MyProperty("key2", "stringValue", "StringLiteral", null, null);
        Property result = factory(prop);

        assertThat(result.getKey(), is("key2"));
        assertThat(result.getStringLiteralValue().getValue(), is("stringValue"));
    }

    @Test
    void testFactoryWithLangLiteral() {
        MyProperty prop = new MyProperty("key3", "langValue", "LangLiteral", "en", null);
        Property result = factory(prop);

        assertThat(result.getKey(), is("key3"));
        assertThat(result.getLangLiteralValue().getValue(), is("langValue"));
        assertThat(result.getLangLiteralValue().getLang(), is("en"));
    }

    @Test
    void testFactoryWithLiteral() {
        MyProperty prop = new MyProperty("key4", "literalValue", "Literal", null, "string");
        Property result = factory(prop);

        assertThat(result.getKey(), is("key4"));
        assertThat(result.getLiteralValue().getValue(), is("literalValue"));
        assertThat(result.getLiteralValue().getDataType(), is("string"));
    }

    @Test
    void testFactoryWithLiteralAndCustomDataType() {
        MyProperty prop = new MyProperty("key5", "literalValue", "Literal", null, "http://example.com#customType");
        Property result = factory(prop);

        assertThat(result.getKey(), is("key5"));
        assertThat(result.getLiteralValue().getValue(), is("literalValue"));
        assertThat(result.getLiteralValue().getDataType(), is("customType"));
    }

    @Test
    void testFactoryWithInvalidType() {
        MyProperty prop = new MyProperty("key6", "invalidValue", "InvalidType", null, null);

        IllegalArgumentException t = assertThrows(IllegalArgumentException.class, () -> {
            factory(prop);
        });

        assertThat(t.getMessage(), is(equalTo("invalid property type: InvalidType")));
    }

    @Test
    void testValuesCreation() {
        MyValue myValue = new MyValue("Test Comment", "Test DataType", "Test Label");

        Value value = factory(myValue);

        assertThat(value.getComment(), is(myValue.comment()));
        assertThat(value.getDataType(), is(myValue.dataType()));
        assertThat(value.getLabel(), is(myValue.label()));
    }


    @Test
    void testFactoryWithLiteralValue() {
        Property property = Property.newBuilder().setKey("key1").setLiteralValue(Literal.newBuilder().setDataType("dt").setValue("literalValue").build()).build();
        MyProperty result = factory(property);

        assertThat(result.key(), is("key1"));
        assertThat(result.dataType(), is("dt"));
        assertThat(result.value(), is("literalValue"));
    }

    @Test
    void testFactoryWithLangLiteralValue() {
        Property property = Property.newBuilder().setKey("key2").setLangLiteralValue(LangLiteral.newBuilder().setValue("langLiteralValue").setLang("bo").build()).build();
        MyProperty result = factory(property);

        assertThat(result.key(), is("key2"));
        assertThat(result.lang(), is("bo"));
        assertThat(result.value(), is("langLiteralValue"));
    }

    @Test
    void testFactoryWithUriValue() {
        Property property = Property.newBuilder().setKey("key3").setUriValue(Uri.newBuilder().setValue("http://example.com").build()).build();
        MyProperty result = factory(property);

        assertThat(result.key(), is("key3"));
        assertThat(result.value(), is("http://example.com"));
    }

    @Test
    void testFactoryWithStringLiteralValue() {
        Property property = Property.newBuilder().setKey("key4").setStringLiteralValue(StringLiteral.newBuilder().setValue("stringLiteralValue").build()).build();
        MyProperty result = factory(property);

        assertThat(result.key(), is("key4"));
        assertThat(result.value(), is("stringLiteralValue"));
    }

    @Test
    void testFactoryWithInvalidProperty() {
        Property property = Property.newBuilder().setKey("key5").build();

        assertThrows(IllegalArgumentException.class, () -> {
            factory(property);
        });
    }
}