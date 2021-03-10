package com.streever.iot.data.utility.generator.fields;

public class FieldProperties implements Comparable<FieldProperties> {
    private String name;
    private FieldBase field;
    private boolean number = Boolean.FALSE;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public FieldBase getField() {
        return field;
    }

    public void setField(String name) {
        this.name = field.getName();
    }

    public boolean isNumber() {
        return number;
    }

    public FieldProperties(String name) {
        this.name = name;
    }

    public FieldProperties(String name, FieldBase field) {
        this.name = name;
        this.field = field;
        this.number = field.isNumber();
    }

    public FieldProperties(FieldBase field) {
        this.name = field.getName();
        this.number = field.isNumber();
        this.field = field;
    }

    @Override
    public int compareTo(FieldProperties o) {
        int rtn = this.getName().compareTo(o.getName());
        return rtn;
    }

    @Override
    public String toString() {
        return name;
    }
}
