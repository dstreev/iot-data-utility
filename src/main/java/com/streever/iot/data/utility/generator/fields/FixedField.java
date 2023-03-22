package com.streever.iot.data.utility.generator.fields;

public class FixedField extends FieldBase<String> {
    private String value;

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String getNext() {
        setLast(value);
        return value;
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.STRING;
    }
}
