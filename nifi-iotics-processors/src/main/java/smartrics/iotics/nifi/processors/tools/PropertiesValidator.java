package smartrics.iotics.nifi.processors.tools;

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

import java.util.ArrayList;
import java.util.List;

public class PropertiesValidator implements Validator {
    @Override
    public ValidationResult validate(String subject, String input, ValidationContext context) {
        ValidationResult.Builder builder = new ValidationResult.Builder();
        try {
            List<String> reasons = new ArrayList<>();
            JsonArray obj = JsonParser.parseString(input).getAsJsonArray();
            obj.forEach(jsonElement -> {
                try {
                    JsonToProperty.fromJson(jsonElement.getAsJsonObject());
                } catch (Exception e) {
                    reasons.add(e.getMessage());
                }
            });
            if (reasons.isEmpty()) {
                return builder.subject(subject).input(input).valid(true).build();
            }
            return builder.subject(subject).input(input).valid(false).explanation(String.join(", ", reasons)).build();
        } catch (Exception e) {
            return builder.subject(subject).input(input).valid(false).explanation("Exception when validating property: " + e.getMessage()).build();
        }
    }
}
