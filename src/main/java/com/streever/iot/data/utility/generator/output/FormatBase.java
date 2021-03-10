package com.streever.iot.data.utility.generator.output;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.streever.iot.data.utility.generator.fields.FieldProperties;

import java.io.IOException;
import java.util.Map;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = CSVFormat.class, name = "csv")
        , @JsonSubTypes.Type(value = JSONFormat.class, name = "json")
//        , @JsonSubTypes.Type(value = AVROFormat.class, name = "avro")
})
public abstract class FormatBase implements Cloneable {
    public abstract String getExtension();
    public abstract String write(Map<FieldProperties, Object> record) throws IOException;

    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}


