package smartrics.iotics.nifi.processors.tools;

import smartrics.iotics.connectors.twins.annotations.XsdDatatype;

import java.net.URI;
import java.time.*;
import java.time.format.DateTimeParseException;
import java.util.Optional;

public class TypeDetector {

    public static Optional<XsdDatatype> detectAbsoluteUri(String maybeUri) {
        URI uri = URI.create(maybeUri);
        if (uri.isAbsolute()) {
            return Optional.of(XsdDatatype.anyURI);
        }
        return Optional.empty();
    }

    public static Optional<XsdDatatype> detectDateTimeType(String maybeADateTime) {
        try {
            // Try LocalTime (e.g., "15:24:00")
            LocalTime.parse(maybeADateTime);
            return Optional.of(XsdDatatype.time);
        } catch (DateTimeParseException e1) {
            try {
                // Try LocalDate (e.g., "2024-05-07")
                LocalDate.parse(maybeADateTime);
                return Optional.of(XsdDatatype.date);
            } catch (DateTimeParseException e2) {
                try {
                    // Try LocalDateTime (e.g., "2024-05-07T15:24:00")
                    LocalDateTime.parse(maybeADateTime);
                    return Optional.of(XsdDatatype.dateTime);
                } catch (DateTimeParseException e3) {
                    try {
                        // Try OffsetDateTime (e.g., "2024-05-07T15:24:00+02:00")
                        OffsetDateTime.parse(maybeADateTime);
                        return Optional.of(XsdDatatype.dateTime);
                    } catch (DateTimeParseException e4) {
                        try {
                            // Try ZonedDateTime (e.g., "2024-05-07T15:24:00Z")
                            ZonedDateTime.parse(maybeADateTime);
                            return Optional.of(XsdDatatype.dateTime);
                        } catch (DateTimeParseException e5) {
                            return Optional.empty();
                        }
                    }
                }
            }
        }
    }
}
