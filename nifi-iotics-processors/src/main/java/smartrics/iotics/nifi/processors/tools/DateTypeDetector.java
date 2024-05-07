package smartrics.iotics.nifi.processors.tools;

import smartrics.iotics.connectors.twins.annotations.XsdDatatype;

import java.time.*;
import java.time.format.DateTimeParseException;
import java.util.Optional;

public class DateTypeDetector {
    public static Optional<XsdDatatype> detectDateTimeType(String dateStr) {
        try {
            // Try LocalTime (e.g., "15:24:00")
            LocalTime.parse(dateStr);
            return Optional.of(XsdDatatype.time);
        } catch (DateTimeParseException e1) {
            try {
                // Try LocalDate (e.g., "2024-05-07")
                LocalDate.parse(dateStr);
                return Optional.of(XsdDatatype.date);
            } catch (DateTimeParseException e2) {
                try {
                    // Try LocalDateTime (e.g., "2024-05-07T15:24:00")
                    LocalDateTime.parse(dateStr);
                    return Optional.of(XsdDatatype.dateTime);
                } catch (DateTimeParseException e3) {
                    try {
                        // Try OffsetDateTime (e.g., "2024-05-07T15:24:00+02:00")
                        OffsetDateTime.parse(dateStr);
                        return Optional.of(XsdDatatype.dateTime);
                    } catch (DateTimeParseException e4) {
                        try {
                            // Try ZonedDateTime (e.g., "2024-05-07T15:24:00Z")
                            ZonedDateTime.parse(dateStr);
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
