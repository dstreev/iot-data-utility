package com.streever.iot.data.utility.generator.output;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = CSVOutput.class, name = "csv")
        , @JsonSubTypes.Type(value = JSONOutput.class, name = "json")
        , @JsonSubTypes.Type(value = StdOutput.class, name = "std")
})
@JsonIgnoreProperties({ "name" })
public abstract class OutputBase implements Output, Cloneable {
    private String name = null;
    private boolean used = Boolean.FALSE;
    private boolean open = false;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

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
