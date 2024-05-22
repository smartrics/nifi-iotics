package smartrics.iotics.nifi.processors.objects;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public class MyValue {

    private final String label;
    private final String dataType;
    private final String comment;
    private String value;

    public MyValue(@NotNull String label, String dataType, String comment, String value) {
        Objects.requireNonNull(label);
        this.label = label;
        this.dataType = dataType;
        this.comment = comment;
        this.value = value;
    }

    public MyValue(@NotNull String label) {
        this(label, null, null, null);
    }

    public MyValue(@NotNull String label, String dataType, String comment) {
        this(label, dataType, comment, null);
    }

    public void updateValue(String newValue) {
        this.value = newValue;
    }

    public String label() {
        return label;
    }

    public String dataType() {
        return dataType;
    }

    public String comment() {
        return comment;
    }

    public String value() {
        return value;
    }

    @Override
    public String toString() {
        return "MyValue{" +
                "label='" + label + '\'' +
                ", dataType='" + dataType + '\'' +
                ", comment='" + comment + '\'' +
                ", value='" + value + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MyValue myValue = (MyValue) o;
        return Objects.equals(label, myValue.label) && Objects.equals(dataType, myValue.dataType) && Objects.equals(comment, myValue.comment) && Objects.equals(value, myValue.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(label, dataType, comment, value);
    }
}
