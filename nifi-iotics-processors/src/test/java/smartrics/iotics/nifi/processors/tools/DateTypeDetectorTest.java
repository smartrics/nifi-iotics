package smartrics.iotics.nifi.processors.tools;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DateTypeDetectorTest {

    @ParameterizedTest
    @CsvSource({
            "2024-05-07, date",
            "15:24:00, time",
            "2024-05-07T15:24:00, dateTime",
            "2024-05-07T15:24:00+02:00, dateTime",
            "2024-05-07T15:24:00Z, dateTime"
    })
    void testParsing(String value, String expected) {
        assertThat(DateTypeDetector.detectDateTimeType(value).orElseThrow().toString(), is(equalTo(expected)));
    }


    @Test
    void wontParseInvalid() {
        assertTrue(DateTypeDetector.detectDateTimeType("foo").isEmpty());
    }
}