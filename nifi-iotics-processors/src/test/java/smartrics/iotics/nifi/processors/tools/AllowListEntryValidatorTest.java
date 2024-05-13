package smartrics.iotics.nifi.processors.tools;

import org.apache.nifi.components.ValidationResult;
import org.junit.jupiter.api.Test;
import smartrics.iotics.host.UriConstants;

import static org.junit.jupiter.api.Assertions.*;

class AllowListEntryValidatorTest {

    @Test
    void validateAll() {
        AllowListEntryValidator v = new AllowListEntryValidator();
        ValidationResult result = v.validate("subject", UriConstants.IOTICSProperties.HostAllowListValues.ALL.toString(), null);
        assertTrue(result.isValid());
    }

    @Test
    void validateNone() {
        AllowListEntryValidator v = new AllowListEntryValidator();
        ValidationResult result = v.validate("subject", UriConstants.IOTICSProperties.HostAllowListValues.NONE.toString(), null);
        assertTrue(result.isValid());
    }

    @Test
    void validateListOfDIDs() {
        AllowListEntryValidator v = new AllowListEntryValidator();
        ValidationResult result = v.validate("subject", " did:iotics:1, did:iotics:2 ", null);
        assertTrue(result.isValid());
    }

    @Test
    void validateListOfDIDsWithBlanks() {
        AllowListEntryValidator v = new AllowListEntryValidator();
        ValidationResult result = v.validate("subject", " did:iotics:1, ,, ,did:iotics:2, ", null);
        assertTrue(result.isValid());
    }

    @Test
    void wontValidateListOfDIDsWithWrongs() {
        AllowListEntryValidator v = new AllowListEntryValidator();
        ValidationResult result = v.validate("subject", "did:iotics:1,did:wot:z,did:iotics:2", null);
        assertFalse(result.isValid());
    }

    @Test
    void wontValidateListOfEmpties() {
        AllowListEntryValidator v = new AllowListEntryValidator();
        ValidationResult result = v.validate("subject", ", ,,, ,, , ,, ,", null);
        assertFalse(result.isValid());
    }

    @Test
    void wontValidateAnyString() {
        AllowListEntryValidator v = new AllowListEntryValidator();
        ValidationResult result = v.validate("subject", "ksjfzgfhisu", null);
        assertFalse(result.isValid());
    }

    @Test
    void wontValidateEmpty() {
        AllowListEntryValidator v = new AllowListEntryValidator();
        ValidationResult result = v.validate("subject", "", null);
        assertFalse(result.isValid());
    }
}