package smartrics.iotics.nifi.processors.objects;

import com.iotics.api.*;

import java.net.URI;

public record MyProperty(String key, String value, String type, String lang, String dataType) {

    public MyProperty(String key, Uri uri) {
        this(key, uri.getValue(), "Uri", null, null);
    }

    public MyProperty(String key, LangLiteral value) {
        this(key, value.getValue(), "LangLiteral", value.getLang(), null);
    }

    public MyProperty(String key, Literal value) {
        this(key, value.getValue(), "Literal", null, value.getDataType());
    }

    public MyProperty(String key, StringLiteral value) {
        this(key, value.getValue(), "StringLiteral", null, null);
    }

    public static MyProperty factory(Property property) {
        if(property == null){
            throw new IllegalArgumentException("null property");
        }
        String key = property.getKey();
        if (property.hasLiteralValue())
            return new MyProperty(key, property.getLiteralValue());
        if (property.hasLangLiteralValue())
            return new MyProperty(key, property.getLangLiteralValue());
        if (property.hasUriValue())
            return new MyProperty(key, property.getUriValue());
        if (property.hasStringLiteralValue())
            return new MyProperty(key, property.getStringLiteralValue());
        throw new IllegalArgumentException("invalid property type, missing value: " + property.getKey());
    }

    public static Value factory(MyValue val) {
        return Value.newBuilder()
                .setComment(val.comment())
                .setDataType(val.dataType())
                .setLabel(val.label())
                .build();
    }

    public static Property factory(MyProperty prop) {
        String key = prop.key();
        if(key == null) {
            throw new IllegalArgumentException("invalid property: missing key. " + prop);
        }
        Property.Builder pBuilder = Property.newBuilder().setKey(key);
        String objValue = prop.value();
        if(objValue == null) {
            throw new IllegalArgumentException("invalid property: missing value. " + prop);
        }

        switch (prop.type()) {
            case "Uri" -> pBuilder.setUriValue(Uri.newBuilder().setValue(prop.value()).build());
            case "StringLiteral" -> pBuilder.setStringLiteralValue(StringLiteral.newBuilder()
                    .setValue(prop.value()).build());
            case "LangLiteral" -> pBuilder.setLangLiteralValue(LangLiteral.newBuilder()
                    .setValue(objValue)
                    .setLang(prop.lang())
                    .build());
            case "Literal" -> {
                Literal.Builder lBuilder = Literal.newBuilder()
                        .setDataType("string") // default
                        .setValue(objValue);
                if (prop.dataType() != null) {
                    URI dataTypeUri = URI.create(prop.dataType());
                    if (dataTypeUri.isAbsolute()) {
                        lBuilder.setDataType(dataTypeUri.getRawFragment());
                    } else {
                        lBuilder.setDataType(prop.dataType());
                    }
                }
                pBuilder.setLiteralValue(lBuilder.build());
            }
            case null, default -> throw new IllegalArgumentException("invalid property type: " + prop.type());
        }
        return pBuilder.build();
    }

}
