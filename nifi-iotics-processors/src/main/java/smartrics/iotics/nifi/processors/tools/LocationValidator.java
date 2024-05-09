package smartrics.iotics.nifi.processors.tools;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

import java.util.ArrayList;
import java.util.List;

public class LocationValidator implements Validator {
    @Override
    public ValidationResult validate(String subject, String input, ValidationContext context) {
        ValidationResult.Builder builder = new ValidationResult.Builder();
        try {
            JsonObject obj = JsonParser.parseString(input).getAsJsonObject();
            List<String> reasons = new ArrayList<>();
            JsonElement radiusElement = obj.get("r");
            if (radiusElement == null || radiusElement.getAsInt() <= 0) {
                reasons.add("missing or invalid radius");
            }
            JsonElement latElement = obj.get("lat");
            if (latElement == null || latElement.getAsDouble() < -90 || latElement.getAsDouble() > 90) {
                reasons.add("missing or invalid lat");
            }
            JsonElement lonElement = obj.get("lon");
            if (lonElement == null || lonElement.getAsDouble() < -180 || lonElement.getAsDouble() > 180) {
                reasons.add("missing or invalid lon");
            }
            if (!reasons.isEmpty()) {
                String reason = String.join(", ", reasons);
                return builder.subject(subject).input(input).valid(false).explanation(reason).build();
            }
            return builder.subject(subject).input(input).valid(true).build();
        } catch (Exception e) {
            return builder.subject(subject).input(input).valid(false).explanation("Exception when validating JSON: " + e.getMessage()).build();
        }
    }
}
