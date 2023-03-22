package com.streever.iot.data.utility.generator.output;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.streever.iot.data.utility.generator.fields.FieldProperties;

import java.util.Map;

public class YAMLFormat extends JSONFormat {

    @Override
    public String getExtension() {
        return "yaml";
    }

    @Override
    public String format(ObjectNode node) {
        String rtn = null;
            rtn = "TODO";
        return rtn;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        YAMLFormat clone = (YAMLFormat)super.clone();

        return clone;
    }

}
