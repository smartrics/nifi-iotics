package smartrics.iotics.nifi.processors.objects;

import com.iotics.api.Property;
import smartrics.iotics.connectors.twins.Mapper;

import java.util.Optional;

public class Utils {
    public static Optional<Property> find(Mapper twin, String key, String type) {
        return twin.getUpsertTwinRequest().getPayload().getPropertiesList().stream().filter(p ->
                        p.getKey().equals(key) &&
                                p.hasLiteralValue() &&
                                p.getLiteralValue().getDataType().equals(type))
                .findFirst();
    }

    public static Optional<Property> findURI(Mapper twin, String key) {
        return twin.getUpsertTwinRequest().getPayload().getPropertiesList().stream().filter(p ->
                        p.getKey().equals(key) &&
                                p.hasUriValue())
                .findFirst();
    }

    public static Optional<Property> findLang(Mapper twin, String key, String lang) {
        return twin.getUpsertTwinRequest().getPayload().getPropertiesList().stream().filter(p ->
                        p.getKey().equals(key) &&
                                p.hasLangLiteralValue() &&
                                p.getLangLiteralValue().getLang().equals(lang))
                .findFirst();
    }

}
