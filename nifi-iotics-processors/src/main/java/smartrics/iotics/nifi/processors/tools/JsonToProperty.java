package smartrics.iotics.nifi.processors.tools;

import com.google.gson.JsonObject;
import com.iotics.api.*;

public class JsonToProperty {

    public static Property fromJson(JsonObject o) {
        Property.Builder b = Property.newBuilder();
        if(!o.has("key")) {
            throw new IllegalArgumentException("Invalid property: missing \"key\"");
        }
        b.setKey(o.get("key").getAsString());
        if (o.has("literal")) {
            Literal.Builder lb = Literal.newBuilder().setValue(o.get("literal").getAsString());
            if (o.has("dataType")) {
                lb.setDataType(o.get("dataType").getAsString());
            } else {
                throw new IllegalArgumentException("Invalid property: literal without \"dataType\"");
            }
            b.setLiteralValue(lb.build());
        } else if (o.has("langLiteral")) {
            LangLiteral.Builder lb = LangLiteral.newBuilder().setValue(o.get("langLiteral").getAsString());
            if (o.has("lang")) {
                lb.setLang(o.get("lang").getAsString());
            } else {
                throw new IllegalArgumentException("Invalid property: langLiteral without \"lang\"");
            }
            b.setLangLiteralValue(lb.build());
        } else if (o.has("stringLiteral")) {
            b.setStringLiteralValue(StringLiteral.newBuilder().setValue(o.get("stringLiteral").getAsString()).build());
        } else if (o.has("uri")) {
            b.setUriValue(Uri.newBuilder().setValue(o.get("uri").getAsString()).build());
        } else {
            throw new IllegalArgumentException("Invalid property: missing one of \"uri\", \"literal\", \"stringLiteral\", \"langLiteral\" ");
        }
        return b.build();
    }

}
