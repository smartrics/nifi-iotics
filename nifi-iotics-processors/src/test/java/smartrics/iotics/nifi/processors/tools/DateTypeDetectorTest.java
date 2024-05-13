package smartrics.iotics.nifi.processors.tools;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

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
        assertThat(TypeDetector.detectDateTimeType(value).orElseThrow().toString(), is(equalTo(expected)));
    }


    @Test
    void wontParseInvalid() {
        assertTrue(TypeDetector.detectDateTimeType("foo").isEmpty());
    }

    @Test
    void detectsAbsoluteURIs() {
        String url = "http://smartrics.it";
        assertThat(TypeDetector.detectAbsoluteUri(url).orElseThrow().toString(), is(equalTo("anyURI")));
    }

    @Test
    void wontDetectURIIfNotAbsolute() {
        String url = "smartrics.it";
        assertTrue(TypeDetector.detectAbsoluteUri(url).isEmpty());
    }
}