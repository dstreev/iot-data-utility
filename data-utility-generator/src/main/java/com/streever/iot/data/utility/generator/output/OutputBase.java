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
public abstract class OutputBase implements Output, Cloneable {
    private boolean used = Boolean.FALSE;
    private boolean open = false;

    public boolean isOpen() {
        return open;
    }

    public void setOpen(boolean open) {
        this.open = open;
    }

    public boolean isUsed() {
        return used;
    }

    public void setUsed(boolean used) {
        this.used = used;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
