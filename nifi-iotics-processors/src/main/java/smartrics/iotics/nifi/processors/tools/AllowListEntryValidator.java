package smartrics.iotics.nifi.processors.tools;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.jetbrains.annotations.NotNull;
import smartrics.iotics.host.UriConstants;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AllowListEntryValidator implements Validator {
    @Override
    public ValidationResult validate(String subject, String value, ValidationContext context) {
        Optional<String> validString = toValidString(value);
        return (new ValidationResult.Builder())
                .subject(subject)
                .input(value)
                .valid(validString.isPresent())
                .explanation(subject + " must be one of 'ALL', 'NONE', list of  host DIDs separate by comma").build();
    }

    public static Optional<String> toValidString(String value) {
        if(value == null || value.isBlank()) {
            return Optional.empty();
        }
        try {
            Optional<String> found = Arrays.stream(UriConstants.IOTICSProperties.HostAllowListValues.values())
                    .map(UriConstants.IOTICSProperties.HostAllowListValues::toString)
                    .filter(string -> string.equals(value)).findFirst();
            if(found.isPresent()) {
                return found;
            }
        } catch (RuntimeException ignored) {
        }
        return parse(value);
    }

    private static @NotNull Optional<String> parse(String value) {
        String[] DIDs = value.split(",");
        Stream<String> stringStream = Arrays.stream(DIDs).map(String::trim).filter(p -> !p.isBlank());
        Function<String, Boolean> isValid = s -> s.startsWith("did:iotics:");
        Map<Boolean, List<String>> split = stringStream.collect(Collectors.groupingBy(isValid));
        List<String> invalidDiDs = split.get(Boolean.FALSE);
        if(invalidDiDs != null) {
            return Optional.empty();
        }
        List<String> validDiDs = split.get(Boolean.TRUE);
        if(validDiDs == null) {
            return Optional.empty();
        }
        String res = String.join(",", validDiDs);
        if(res.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(res);
    }
}
