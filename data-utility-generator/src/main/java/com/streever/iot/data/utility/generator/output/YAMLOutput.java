package com.streever.iot.data.utility.generator.output;

import com.streever.iot.data.utility.generator.Record;
import com.streever.iot.data.utility.generator.fields.FieldBase;

import java.util.Map;

public class YAMLOutput extends FileOutput {

    @Override
    protected String getExtension() {
        return "yaml";
    }

    @Override
    public void write(Map<FieldBase, Object> record) {

    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        YAMLOutput clone = (YAMLOutput)super.clone();

        return clone;
    }

}
