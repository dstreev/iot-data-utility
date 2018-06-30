package com.streever.iot.data.utility.generator.fields;

public interface ControlField {
    boolean terminate();
    boolean isControlField();
    void setControlField(Boolean controlField);
}
