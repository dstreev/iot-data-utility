package com.streever.iot.data.utility.generator;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.streever.iot.data.utility.generator.output.OutputBase;

import java.util.Map;
import java.util.TreeMap;

public class OutputSpec {
    private OutputBase default_;
    private Map<String, OutputBase> relationships = new TreeMap<String, OutputBase>();

    public void setDefault(OutputBase default_) {
        this.default_ = default_;
    }

    @JsonProperty("default")
    public OutputBase getDefault() {
        return default_;
    }

    public Map<String, OutputBase> getRelationships() {
        return relationships;
    }

    public void setRelationships(Map<String, OutputBase> relationships) {
        this.relationships = relationships;
    }
}
