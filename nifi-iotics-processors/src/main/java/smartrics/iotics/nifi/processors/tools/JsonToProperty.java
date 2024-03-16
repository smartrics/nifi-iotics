package smartrics.iotics.nifi.processors.tools;

import com.google.gson.JsonObject;
import com.iotics.api.*;

public class JsonToProperty {

    public static Property fromJson(JsonObject o) {
        Property.Builder b = Property.newBuilder();
        b.setKey(o.get("key").getAsString());
        if(o.has("literal")) {
            Literal.Builder lb = Literal.newBuilder().setValue(o.get("literal").getAsString());
            if(o.has("dataType")) {
                lb.setDataType(o.get("dataType").getAsString());
            }
            b.setLiteralValue(lb.build());
        }
        if(o.has("")) {
            LangLiteral.Builder lb = LangLiteral.newBuilder().setValue(o.get("langLiteral").getAsString());
            if(o.has("lang")) {
                lb.setLang(o.get("lang").getAsString());
            }
            b.setLangLiteralValue(lb.build());
        }
        if(o.has("stringLiteral")) {
            b.setStringLiteralValue(StringLiteral.newBuilder().setValue(o.get("stringLiteral").getAsString()).build());
        }
        if(o.has("uri")) {
            b.setUriValue(Uri.newBuilder().setValue(o.get("uri").getAsString()).build());
        }
        return b.build();
    }

}
