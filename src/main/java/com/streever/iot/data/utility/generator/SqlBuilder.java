package com.streever.iot.data.utility.generator;

public interface SqlBuilder {
    String build();
    void link();
    void setSchema(Schema schema);
    Schema getSchema();
}
