package com.streever.iot.data.utility.generator.output;

import com.streever.iot.data.utility.generator.Record;
import com.streever.iot.data.utility.generator.fields.FieldBase;
import com.streever.iot.data.utility.generator.fields.FieldProperties;

import java.util.Map;

public class YAMLOutput extends JSONOutput {

    @Override
    protected String getExtension() {
        return "yaml";
    }

    @Override
    public long write(Map<FieldProperties, Object> record) {
        return 0;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        YAMLOutput clone = (YAMLOutput)super.clone();

        return clone;
    }

}
