package com.streever.iot.data.utility.generator.output;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.streever.iot.data.utility.generator.output.kafka.KafkaOutput;

import java.io.IOException;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = LocalFileOutput.class, name = "local")
        , @JsonSubTypes.Type(value = DFSOutput.class, name = "dfs")
        , @JsonSubTypes.Type(value = StdOutput.class, name = "std")
        , @JsonSubTypes.Type(value = KafkaOutput.class, name = "kafka")
})
@JsonIgnoreProperties({ "name" })
public abstract class OutputBase implements Output, Cloneable {
    private String name = null;
    private boolean used = Boolean.FALSE;
    private boolean open = false;
    private FormatBase format = new CSVFormat();

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

    public FormatBase getFormat() {
        return format;
    }

    public void setFormat(FormatBase format) {
        this.format = format;
    }

    protected abstract void writeLine(String line) throws IOException;

    public long write(ObjectNode node) throws IOException {
        String line = format.format(node);
        writeLine(line);
        long rtn = line.length() + 1;
        return rtn;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        OutputBase clone = (OutputBase)super.clone();
        if (this.format != null) {
            clone.setFormat((FormatBase)this.format.clone());
        }
        return clone;
    }
}
