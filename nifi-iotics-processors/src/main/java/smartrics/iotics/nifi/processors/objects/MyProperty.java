package smartrics.iotics.nifi.processors.objects;

import com.iotics.api.*;

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

    public static MyProperty Factory(Property property) {
        String key = property.getKey();
        if (property.hasLiteralValue())
            return new MyProperty(key, property.getLiteralValue());
        if (property.hasLangLiteralValue())
            return new MyProperty(key, property.getLangLiteralValue());
        if (property.hasUriValue())
            return new MyProperty(key, property.getUriValue());
        if (property.hasStringLiteralValue())
            return new MyProperty(key, property.getStringLiteralValue());
        throw new IllegalArgumentException("invalid property type: " + property);
    }

}
