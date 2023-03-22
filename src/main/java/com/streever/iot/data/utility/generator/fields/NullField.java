package com.streever.iot.data.utility.generator.fields;

public class NullField extends FieldBase<String> {
    private String nullValue = "";

    @Override
    public FieldType getFieldType() {
        return FieldType.STRING;
    }

    public String getNullValue() {
        return nullValue;
    }

    public void setNullValue(String nullValue) {
        this.nullValue = nullValue;
    }

    @Override
    public String getNext() {
        setLast(nullValue);
        return nullValue;
    }
}
