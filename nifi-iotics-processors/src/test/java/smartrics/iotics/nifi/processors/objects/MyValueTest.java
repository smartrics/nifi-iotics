package smartrics.iotics.nifi.processors.objects;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class MyValueTest {

    @Test
    void testConstructorWithAllParameters() {
        MyValue myValue = new MyValue("label1", "dataType1", "comment1", "value1");

        assertEquals("label1", myValue.label());
        assertEquals("dataType1", myValue.dataType());
        assertEquals("comment1", myValue.comment());
        assertEquals("value1", myValue.value());
    }

    @Test
    void testConstructorWithLabelOnly() {
        MyValue myValue = new MyValue("label2");

        assertEquals("label2", myValue.label());
        assertNull(myValue.dataType());
        assertNull(myValue.comment());
        assertNull(myValue.value());
    }

    @Test
    void testConstructorWithLabelDataTypeComment() {
        MyValue myValue = new MyValue("label3", "dataType3", "comment3");

        assertEquals("label3", myValue.label());
        assertEquals("dataType3", myValue.dataType());
        assertEquals("comment3", myValue.comment());
        assertNull(myValue.value());
    }

    @Test
    void testUpdateValue() {
        MyValue myValue = new MyValue("label4", "dataType4", "comment4", "value4");
        myValue.updateValue("newValue");

        assertEquals("newValue", myValue.value());
    }

    @Test
    void testToString() {
        MyValue myValue = new MyValue("label5", "dataType5", "comment5", "value5");
        String expectedString = "MyValue{label='label5', dataType='dataType5', comment='comment5', value='value5'}";

        assertEquals(expectedString, myValue.toString());
    }

    @Test
    void testEqualsAndHashCode() {
        MyValue myValue1 = new MyValue("label6", "dataType6", "comment6", "value6");
        MyValue myValue2 = new MyValue("label6", "dataType6", "comment6", "value6");
        MyValue myValue3 = new MyValue("label7", "dataType7", "comment7", "value7");

        assertEquals(myValue1, myValue2);
        assertNotEquals(myValue1, myValue3);
        assertEquals(myValue1.hashCode(), myValue2.hashCode());
        assertNotEquals(myValue1.hashCode(), myValue3.hashCode());
    }

    @Test
    void testConstructorWithNullLabelThrowsException() {
        assertThrows(NullPointerException.class, () -> {
            new MyValue(null, "dataType8", "comment8", "value8");
        });
    }
}
