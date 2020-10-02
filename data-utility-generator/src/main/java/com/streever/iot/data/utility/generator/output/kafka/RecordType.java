package com.streever.iot.data.utility.generator.output.kafka;

public class RecordType implements Cloneable {
    private String key = "String";
    private String value = "String";

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        RecordType clone = (RecordType)super.clone();
        clone.setKey(new String(this.key));
        clone.setValue(new String(this.value));
        return clone;
    }
}
