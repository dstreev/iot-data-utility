package com.streever.iot.data.utility.generator.output;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = CSVOutput.class, name = "csv")
        , @JsonSubTypes.Type(value = JSONOutput.class, name = "json")
        , @JsonSubTypes.Type(value = StdOutOutput.class, name = "stdout")
        , @JsonSubTypes.Type(value = StdErrOutput.class, name = "stderr")
})
public abstract class OutputBase implements Output {
    private boolean used = Boolean.FALSE;

    public boolean isUsed() {
        return used;
    }

    public void setUsed(boolean used) {
        this.used = used;
    }
}
