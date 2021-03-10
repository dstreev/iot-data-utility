package com.streever.iot.data.utility.generator.output;

import com.streever.iot.data.utility.generator.fields.FieldProperties;

import java.util.Map;

public class YAMLFormat extends JSONFormat {

    @Override
    public String getExtension() {
        return "yaml";
    }

    @Override
    public String write(Map<FieldProperties, Object> record) {
        return null;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        YAMLFormat clone = (YAMLFormat)super.clone();

        return clone;
    }

}
