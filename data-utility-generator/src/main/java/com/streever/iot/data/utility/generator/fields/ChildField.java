package com.streever.iot.data.utility.generator.fields;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

//@JsonIgnoreProperties({ "parent" })
public class ChildField extends FieldBase<String> {
    private List<String> keyFields;
    private List<FieldBase> fields;
    public List<String> getKeyFields() {
        return keyFields;
    }
    public void setKeyField(List<String> keyFields) {
        this.keyFields = keyFields;
    }

    @Override
    public String getNext() {
        return null;
    }
}
