package com.streever.iot.data.utility.generator.output;

import com.streever.iot.data.utility.generator.Record;
import com.streever.iot.data.utility.generator.fields.FieldBase;

import java.util.Map;

public class JSONOutput extends FileOutput {

    @Override
    protected String getExtension() {
        return "json";
    }

    @Override
    public void write(Map<FieldBase, Object> record) {

    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        JSONOutput clone = (JSONOutput)super.clone();

        return clone;
    }
}
